# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

# 1. Crear base de datos en el catálogo de Databricks
spark.sql("CREATE DATABASE IF NOT EXISTS bbva_pld_db")
spark.sql("USE bbva_pld_db")

# 2. Catálogo de 10,000 Clientes en México (Perfil Transaccional Declarado)
num_clientes = 10000
df_clientes = (
    spark.range(1, num_clientes + 1)
    .withColumnRenamed("id", "id_cliente_num")
    .withColumn("id_cliente", F.format_string("CLI_%06d", F.col("id_cliente_num")))
    .withColumn(
        "estado",
        F.element_at(
            F.array(
                F.lit("CDMX"), F.lit("Estado de México"), F.lit("Nuevo León"),
                F.lit("Jalisco"), F.lit("Sinaloa"), F.lit("Chihuahua"),
                F.lit("Baja California"), F.lit("Querétaro"), F.lit("Puebla")
            ),
            (F.floor(F.rand(seed=42) * 9) + 1).cast("int")
        )
    )
    .withColumn(
        "segmento",
        F.when(F.rand(seed=10) < 0.70, "Retail")
         .when(F.rand(seed=10) < 0.90, "PyME")
         .otherwise("Patrimonial")
    )
    .withColumn(
        "ingreso_mensual_mxn",
        F.when(F.col("segmento") == "Retail", F.round(F.rand(seed=20) * 35000 + 10000, 2))
         .when(F.col("segmento") == "PyME", F.round(F.rand(seed=20) * 180000 + 50000, 2))
         .otherwise(F.round(F.rand(seed=20) * 500000 + 200000, 2))
    )
    .drop("id_cliente_num")
)

df_clientes.write.format("delta").mode("overwrite").saveAsTable("catalogo_clientes")

# 3. Generación de 1,000,000 de Transacciones (Core Transaccional)
num_tx = 1000000
df_base_tx = (
    spark.range(1, num_tx + 1)
    .withColumnRenamed("id", "tx_num")
    .withColumn("id_transaccion", F.format_string("TX_%08d", F.col("tx_num")))
    .withColumn("id_cliente", F.format_string("CLI_%06d", (F.floor(F.rand(seed=99) * num_clientes) + 1).cast("int")))
    .withColumn(
        "canal",
        F.element_at(
            F.array(F.lit("SPEI"), F.lit("EFECTIVO_VENTANILLA"), F.lit("ATM"), F.lit("POS_TARJETA"), F.lit("SWIFT_INTERNACIONAL")),
            (F.floor(F.rand(seed=33) * 5) + 1).cast("int")
        )
    )
    .withColumn(
        "pais_contraparte",
        F.when(F.col("canal") == "SWIFT_INTERNACIONAL",
               F.element_at(F.array(F.lit("USA"), F.lit("PANAMA"), F.lit("ISLAS_CAIMAN"), F.lit("ESPANA"), F.lit("COLOMBIA")),
                            (F.floor(F.rand(seed=77) * 5) + 1).cast("int")))
         .otherwise(F.lit("MEXICO"))
    )
    # Timestamp distribuido en los últimos 30 días
    .withColumn("segundos_offset", (F.rand(seed=55) * 30 * 86400).cast("long"))
    .withColumn("fecha_hora", F.from_unixtime(F.unix_timestamp(F.lit("2026-10-01 00:00:00")) + F.col("segundos_offset")).cast("timestamp"))
    # Montos base + inyección de anomalías (Pitufeo justo debajo de umbral relevante ~$145,000 MXN / $7,500 USD)
    .withColumn(
        "monto_mxn",
        F.when((F.col("canal") == "EFECTIVO_VENTANILLA") & (F.rand(seed=88) < 0.04),
               F.round(F.lit(138000) + F.rand(seed=12) * 9500, 2))  # Tipología Pitufeo / Estructuración
         .when(F.rand(seed=44) < 0.015,
               F.round(F.rand(seed=15) * 850000 + 250000, 2))        # Montos atípicos altos
         .otherwise(F.round(F.rand(seed=66) * 28000 + 200, 2))       # Operación cotidiana
    )
    # Inyectar 0.2% de nulos intencionales en monto_mxn para probar Calidad de Datos
    .withColumn("monto_mxn", F.when(F.rand(seed=101) < 0.002, F.lit(None)).otherwise(F.col("monto_mxn")))
    .drop("tx_num", "segundos_offset")
)

df_base_tx.write.format("delta").mode("overwrite").saveAsTable("core_transaccional")

# 4. Simular Sistema Contable (con 500 registros faltantes y 1,200 montos alterados para conciliación)
df_contable = (
    df_base_tx
    .filter(F.rand(seed=202) >= 0.0005)  # Omitir 0.05% de transacciones (huérfanas en Core)
    .withColumn(
        "monto_contable_mxn",
        F.when(F.rand(seed=303) < 0.0012, F.col("monto_mxn") + F.lit(150.50))  # Desviación contable
         .otherwise(F.col("monto_mxn"))
    )
    .select("id_transaccion", "monto_contable_mxn")
)

df_contable.write.format("delta").mode("overwrite").saveAsTable("sistema_contable")

print("✅ Tablas Delta creadas exitosamente: catalogo_clientes (10K), core_transaccional (1M), sistema_contable.")

# COMMAND ----------

# Auditoría de Calidad de Datos (Completitud, Unicidad e Integridad)
df_core = spark.table("bbva_pld_db.core_transaccional")
df_cont = spark.table("bbva_pld_db.sistema_contable")

total_registros = df_core.count()
registros_unicos = df_core.select("id_transaccion").distinct().count()
nulos_monto = df_core.filter(F.col("monto_mxn").isNull()).count()
montos_negativos = df_core.filter(F.col("monto_mxn") <= 0).count()

# Conciliación Core Transaccional vs Sistema Contable (Full Outer Join)
df_conciliacion = (
    df_core.alias("core")
    .join(df_cont.alias("cont"), on="id_transaccion", how="full_outer")
    .withColumn(
        "estatus_conciliacion",
        F.when(F.col("core.monto_mxn").isNull(), "ERROR_CALIDAD_NULO_CORE")
         .when(F.col("cont.monto_contable_mxn").isNull(), "DESVIACION_FALTA_EN_CONTABLE")
         .when(F.abs(F.col("core.monto_mxn") - F.col("cont.monto_contable_mxn")) > 0.01, "DESVIACION_DIFERENCIA_MONTO")
         .otherwise("CONCILIADO_OK")
    )
    .withColumn("diferencia_mxn", F.coalesce(F.col("core.monto_mxn"), F.lit(0.0)) - F.coalesce(F.col("cont.monto_contable_mxn"), F.lit(0.0)))
)

# Guardar tabla auditada para reporte de Gobierno de Datos
df_conciliacion.write.format("delta").mode("overwrite").saveAsTable("bbva_pld_db.auditoria_conciliacion")

print("=== REPORTE DE CALIDAD Y CONCILIACIÓN DE DATOS ===")
print(f"Total registros en Core: {total_registros:,}")
print(f"Unicidad de ID_Transaccion: {registros_unicos:,} ({(registros_unicos/total_registros)*100:.2f}%)")
print(f"Registros con monto nulo (Completitud): {nulos_monto:,}")

display(
    df_conciliacion.groupBy("estatus_conciliacion")
    .agg(
        F.count("*").alias("total_operaciones"),
        F.round(F.sum("diferencia_mxn"), 2).alias("impacto_neto_mxn")
    )
    .orderBy(F.desc("total_operaciones"))
)