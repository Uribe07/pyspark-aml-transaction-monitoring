# 📚 Diccionario de Datos, Linaje y Reglas de Negocio (PLD/FT)

## 1. Linaje de Datos (Data Lineage)

```text
[catalogo_clientes (10K)] ──┐
                            ├──► [core_transaccional (1M)] ──► [auditoria_conciliacion] (Control Calidad)
[sistema_contable] ─────────┘               │
                                            ▼
                            [transacciones_evaluadas_pld] (Window Functions + Reglas CNBV/UIF/GAFI)
                                            │
                                            ▼
                            [matriz_riesgo_clientes_pld] (Isolation Forest + K-Means + Dictamen Híbrido)
```

## 2. Catálogo de Tablas Delta Lake (`bbva_pld_db`)

### Tabla Final: `matriz_riesgo_clientes_pld`
| Campo | Tipo | Descripción Funcional | Regla de Calidad / Dominio |
| :--- | :--- | :--- | :--- |
| `id_cliente` | `STRING` | Identificador único del cliente bancario. | Llave primaria (`CLI_XXXXXX`), 100% unicidad. |
| `segmento` | `STRING` | Banca a la que pertenece el cliente. | `Retail`, `PyME`, `Patrimonial`. |
| `estado` | `STRING` | Entidad federativa de operación en México. | 9 estados de la República Mexicana. |
| `ingreso_mensual_mxn` | `DOUBLE` | Perfil transaccional declarado en originación. | Valor > 0 MXN. |
| `monto_total_mes` | `DOUBLE` | Volumen acumulado operado en ventana de 30 días. | Suma de transacciones válidas (`monto_mxn > 0`). |
| `pct_efectivo` | `DOUBLE` | Proporción del volumen operado en ventanilla en efectivo. | Rango `[0.0000 - 1.0000]`. |
| `pct_tx_gafi` | `DOUBLE` | Proporción de operaciones con jurisdicciones GAFI. | `PANAMA`, `ISLAS_CAIMAN`. |
| `max_velocidad_24h` | `LONG` | Máximo número de operaciones en ventana móvil de 24h. | Calculado vía `Window.rangeBetween(-86400, 0)`. |
| `score_anomalia_ml` | `DOUBLE` | Índice normalizado de anomalía (`Isolation Forest`). | Escala `0.00` a `100.00` (mayor = más atípico). |
| `cluster_comportamiento` | `INT` | Grupo asignado por `K-Means` (`k=3`). | Clústeres `0, 1, 2`. |
| `dictamen_hibrido_pld` | `STRING` | Resolución final combinando Reglas + ML. | `P1_CRITICA_ROS_INMEDIATO`, `P2_SOSPECHA_OCULTA_ML`, `P3_REVISION_ESTANDAR_REGLA`, `P4_FALSO_POSITIVO_DESCARTADO`, `SIN_RIESGO_OPERATIVO`. |
