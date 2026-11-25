-- Databricks notebook source
USE bbva_pld_db;

SELECT 
    segmento,
    dictamen_hibrido_pld,
    COUNT(id_cliente) AS total_clientes,
    ROUND(AVG(score_anomalia_ml), 2) AS score_anomalia_promedio,
    ROUND(SUM(monto_total_mes) / 1000000, 2) AS monto_total_millones_mxn,
    SUM(alertas_pitufeo) AS total_alertas_pitufeo,
    SUM(alertas_gafi) AS total_alertas_gafi
FROM matriz_riesgo_clientes_pld
GROUP BY segmento, dictamen_hibrido_pld
ORDER BY segmento, total_clientes DESC;

-- COMMAND ----------

SELECT 
    estado,
    COUNT(DISTINCT CASE WHEN dictamen_hibrido_pld IN ('P1_CRITICA_ROS_INMEDIATO', 'P2_SOSPECHA_OCULTA_ML') THEN id_cliente END) AS clientes_criticos_pld,
    ROUND(SUM(CASE WHEN dictamen_hibrido_pld IN ('P1_CRITICA_ROS_INMEDIATO', 'P2_SOSPECHA_OCULTA_ML') THEN monto_total_mes ELSE 0 END) / 1000000, 2) AS monto_expuesto_millones_mxn,
    ROUND(AVG(pct_efectivo) * 100, 2) AS pct_promedio_efectivo,
    ROUND(AVG(pct_tx_gafi) * 100, 2) AS pct_promedio_gafi
FROM matriz_riesgo_clientes_pld
GROUP BY estado
ORDER BY monto_expuesto_millones_mxn DESC;

-- COMMAND ----------

SELECT 
    id_cliente,
    segmento,
    estado,
    dictamen_hibrido_pld,
    score_anomalia_ml,
    ROUND(ingreso_mensual_mxn, 2) AS ingreso_declarado_mxn,
    ROUND(monto_total_mes, 2) AS operado_mes_mxn,
    ratio_volumen_vs_ingreso,
    alertas_pitufeo,
    alertas_perfil,
    alertas_gafi
FROM matriz_riesgo_clientes_pld
WHERE dictamen_hibrido_pld IN ('P1_CRITICA_ROS_INMEDIATO', 'P2_SOSPECHA_OCULTA_ML')
ORDER BY score_anomalia_ml DESC, operado_mes_mxn DESC
LIMIT 20;