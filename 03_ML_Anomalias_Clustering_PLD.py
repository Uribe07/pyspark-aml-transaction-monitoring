# Databricks notebook source
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

spark.sql("USE bbva_pld_db")

# 1. Agregar comportamiento transaccional a nivel cliente desde el millón de registros
df_perfil_clientes = (
    spark.table("transacciones_evaluadas_pld")
    .groupBy("id_cliente", "segmento", "estado", "ingreso_mensual_mxn")
    .agg(
        F.count("id_transaccion").alias("total_tx"),
        F.round(F.sum("monto_mxn"), 2).alias("monto_total_mes"),
        F.round(F.avg("monto_mxn"), 2).alias("ticket_promedio"),
        F.round(F.max("monto_mxn"), 2).alias("ticket_maximo"),
        F.round(F.stddev("monto_mxn"), 2).alias("desv_estandar_monto"),
        F.round(
            F.sum(F.when(F.col("canal") == "EFECTIVO_VENTANILLA", F.col("monto_mxn")).otherwise(0.0)) /
            F.sum("monto_mxn"),
            4
        ).alias("pct_efectivo"),
        F.round(
            F.sum(F.when(F.col("pais_contraparte").isin("PANAMA", "ISLAS_CAIMAN"), 1).otherwise(0)) /
            F.count("id_transaccion"),
            4
        ).alias("pct_tx_gafi"),
        F.max("tx_count_24h").alias("max_velocidad_24h"),
        F.max("total_reglas_activadas").alias("max_reglas_pld"),
        F.sum("regla_r1_pitufeo").alias("alertas_pitufeo"),
        F.sum("regla_r2_perfil_inusual").alias("alertas_perfil"),
        F.sum("regla_r3_jurisdiccion_gafi").alias("alertas_gafi")
    )
    .fillna(0.0, subset=["desv_estandar_monto"])
    .withColumn("ratio_volumen_vs_ingreso", F.round(F.col("monto_total_mes") / F.col("ingreso_mensual_mxn"), 2))
)

print(f"✅ Perfiles consolidados listos para ML: {df_perfil_clientes.count():,} clientes.")

# COMMAND ----------

# Convertir perfiles agregados (10K clientes) a Pandas para scikit-learn
pdf = df_perfil_clientes.toPandas()

features_ml = [
    "total_tx",
    "monto_total_mes",
    "ticket_maximo",
    "desv_estandar_monto",
    "pct_efectivo",
    "pct_tx_gafi",
    "max_velocidad_24h",
    "ratio_volumen_vs_ingreso"
]

X = pdf[features_ml].values

# 1. Escalamiento robusto frente a valores extremos financieros
scaler = RobustScaler()
X_scaled = scaler.fit_transform(X)

# 2. Modelo No Supervisado 1: Isolation Forest (Detección de Anomalías)
iso_forest = IsolationForest(
    n_estimators=200,
    contamination=0.05,  # Top 5% comportamiento más anómalo
    random_state=42,
    n_jobs=-1
)
pdf["anomalia_iforest"] = np.where(iso_forest.fit_predict(X_scaled) == -1, 1, 0)
# Normalizar score de anomalía entre 0 y 100 (mayor = más anómalo)
raw_scores = -iso_forest.score_samples(X_scaled)
pdf["score_anomalia_ml"] = np.round(((raw_scores - raw_scores.min()) / (raw_scores.max() - raw_scores.min())) * 100, 2)

# 3. Modelo No Supervisado 2: K-Means Clustering (Segmentación Comportamental)
X_cluster = scaler.fit_transform(pdf[["monto_total_mes", "pct_efectivo", "pct_tx_gafi", "ratio_volumen_vs_ingreso"]].values)
kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
pdf["cluster_comportamiento"] = kmeans.fit_predict(X_cluster)
sil_score = silhouette_score(X_cluster, pdf["cluster_comportamiento"])

# 4. Matriz Híbrida (Reglas CNBV + Isolation Forest) y Control de Falsos Positivos
def clasificar_prioridad_pld(row):
    tiene_regla = row["max_reglas_pld"] >= 1
    es_anomalia = row["anomalia_iforest"] == 1
    
    if tiene_regla and es_anomalia:
        return "P1_CRITICA_ROS_INMEDIATO"          # Confirmado por Regla + ML (Máxima certeza)
    elif not tiene_regla and es_anomalia:
        return "P2_SOSPECHA_OCULTA_ML"             # Evadió reglas fijas pero ML detectó patrón atípico
    elif tiene_regla and not es_anomalia and row["segmento"] == "Patrimonial":
        return "P4_FALSO_POSITIVO_DESCARTADO"      # Alerta de regla explicada por perfil Patrimonial normal
    elif tiene_regla:
        return "P3_REVISION_ESTANDAR_REGLA"        # Alerta determinística aislada
    else:
        return "SIN_RIESGO_OPERATIVO"

pdf["dictamen_hibrido_pld"] = pdf.apply(clasificar_prioridad_pld, axis=1)

# Guardar tabla final en Delta Lake para SQL Analytics y Dashboard
df_final_spark = spark.createDataFrame(pdf)
df_final_spark.write.format("delta").mode("overwrite").saveAsTable("bbva_pld_db.matriz_riesgo_clientes_pld")

# Métricas de desempeño y reducción de falsos positivos
total_alertados_solo_reglas = len(pdf[pdf["max_reglas_pld"] >= 1])
falsos_positivos_filtrados = len(pdf[pdf["dictamen_hibrido_pld"] == "P4_FALSO_POSITIVO_DESCARTADO"])
reduccion_fp_pct = (falsos_positivos_filtrados / total_alertados_solo_reglas) * 100

print("=== MÉTRICAS DEL MODELO NO SUPERVISADO Y MATRIZ HÍBRIDA ===")
print(f"Silhouette Score (K-Means k=4): {sil_score:.4f}")
print(f"Clientes alertados solo por reglas tradicionales: {total_alertados_solo_reglas:,}")
print(f"Falsos positivos filtrados por contexto ML/Segmento: {falsos_positivos_filtrados:,} (-{reduccion_fp_pct:.1f}% ruido operativo)")
print(f"Nuevos casos ocultos detectados únicamente por Isolation Forest (P2): {len(pdf[pdf['dictamen_hibrido_pld'] == 'P2_SOSPECHA_OCULTA_ML']):,}")

display(
    df_final_spark.groupBy("dictamen_hibrido_pld")
    .agg(
        F.count("*").alias("total_clientes"),
        F.round(F.avg("score_anomalia_ml"), 2).alias("score_anomalia_promedio"),
        F.round(F.sum("monto_total_mes") / 1e6, 2).alias("volumen_total_millones_mxn")
    )
    .orderBy("dictamen_hibrido_pld")
)