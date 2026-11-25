# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE bbva_pld_db")

# 1. Cargar datos limpios (descartando errores de calidad detectados en Fase 1)
df_tx_limpias = (
    spark.table("core_transaccional")
    .filter(F.col("monto_mxn").isNotNull() & (F.col("monto_mxn") > 0))
    .withColumn("ts_unix", F.unix_timestamp("fecha_hora"))
    .withColumn("hora_dia", F.hour("fecha_hora"))
)

df_clientes = spark.table("catalogo_clientes")

# 2. Cruzar transacciones con el perfil económico declarado del cliente
df_enriquecido = df_tx_limpias.join(df_clientes, on="id_cliente", how="inner")

# 3. Definir ventanas móviles temporales por cliente (PySpark Window Functions)
ventana_24h = Window.partitionBy("id_cliente").orderBy("ts_unix").rangeBetween(-86400, 0)
ventana_7d = Window.partitionBy("id_cliente").orderBy("ts_unix").rangeBetween(-604800, 0)

# 4. Calcular indicadores comportamentales en ventanas móviles
df_features_pld = (
    df_enriquecido
    # Conteo y suma acumulada en 24 horas
    .withColumn("tx_count_24h", F.count("id_transaccion").over(ventana_24h))
    .withColumn("monto_acum_24h", F.round(F.sum("monto_mxn").over(ventana_24h), 2))
    # Conteo y suma acumulada en efectivo en 7 días (clave para Pitufeo CNBV)
    .withColumn(
        "monto_efectivo_7d",
        F.round(
            F.sum(F.when(F.col("canal") == "EFECTIVO_VENTANILLA", F.col("monto_mxn")).otherwise(0.0))
            .over(ventana_7d),
            2
        )
    )
    .withColumn(
        "conteo_efectivo_7d",
        F.sum(F.when(F.col("canal") == "EFECTIVO_VENTANILLA", 1).otherwise(0)).over(ventana_7d)
    )
    # Ratio de desviación respecto al ingreso mensual declarado
    .withColumn("ratio_monto_vs_ingreso", F.round(F.col("monto_mxn") / F.col("ingreso_mensual_mxn"), 2))
)

print("✅ Variables comportamentales y ventanas móviles calculadas sobre 1M de registros.")

# COMMAND ----------

# Evaluación CALIBRADA de reglas determinísticas PLD (CNBV / UIF / GAFI)
df_alertas_pld = (
    df_features_pld
    .withColumn(
        "regla_r1_pitufeo",
        F.when(
            (F.col("canal") == "EFECTIVO_VENTANILLA") &
            (F.col("monto_mxn").between(135000, 148000)) &
            (F.col("conteo_efectivo_7d") >= 3) &          # Calibrado: al menos 3 depósitos estructurados en 7d
            (F.col("monto_efectivo_7d") >= 400000),       # Calibrado: acumulación >= $400K MXN
            1
        ).otherwise(0)
    )
    .withColumn(
        "regla_r2_perfil_inusual",
        F.when(
            (F.col("ratio_monto_vs_ingreso") >= 12.0) &   # Calibrado: >12x su ingreso mensual declarado
            (F.col("monto_mxn") >= 450000),
            1
        ).otherwise(0)
    )
    .withColumn(
        "regla_r3_jurisdiccion_gafi",
        F.when(
            (F.col("canal") == "SWIFT_INTERNACIONAL") &
            (F.col("pais_contraparte").isin("PANAMA", "ISLAS_CAIMAN")) &
            (F.col("monto_mxn") >= 450000),               # Calibrado: transferencias relevantes a paraísos fiscales
            1
        ).otherwise(0)
    )
    .withColumn(
        "regla_r4_velocidad_nocturna",
        F.when(
            (F.col("tx_count_24h") >= 8) &                # Calibrado: ráfaga >= 8 tx en 24h de madrugada
            (F.col("hora_dia").between(1, 4)),
            1
        ).otherwise(0)
    )
    .withColumn(
        "total_reglas_activadas",
        F.col("regla_r1_pitufeo") + F.col("regla_r2_perfil_inusual") +
        F.col("regla_r3_jurisdiccion_gafi") + F.col("regla_r4_velocidad_nocturna")
    )
    .withColumn(
        "tipologia_pld",
        F.when(F.col("regla_r1_pitufeo") == 1, "R1_PITUFEO_ESTRUCTURACION_CNBV")
         .when(F.col("regla_r2_perfil_inusual") == 1, "R2_INUSUALIDAD_PERFIL_UIF")
         .when(F.col("regla_r3_jurisdiccion_gafi") == 1, "R3_JURISDICCION_ALTO_RIESGO_GAFI")
         .when(F.col("regla_r4_velocidad_nocturna") == 1, "R4_VELOCIDAD_NOCTURNA")
         .otherwise("SIN_ALERTA_REGLA")
    )
)

df_alertas_pld.write.format("delta").mode("overwrite").saveAsTable("bbva_pld_db.transacciones_evaluadas_pld")

display(
    df_alertas_pld.groupBy("tipologia_pld")
    .agg(
        F.count("*").alias("total_operaciones"),
        F.countDistinct("id_cliente").alias("clientes_unicos"),
        F.round(F.sum("monto_mxn") / 1e6, 2).alias("monto_total_millones_mxn")
    )
    .orderBy(F.desc("total_operaciones"))
)