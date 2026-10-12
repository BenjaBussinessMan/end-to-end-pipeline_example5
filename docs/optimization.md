# Optimización de consultas

## Técnicas aplicadas

| Técnica | Dónde | Efecto esperado |
|---|---|---|
| Liquid Clustering `(event_date, store_id)` | `silver.transactions` | Data skipping por fecha/tienda sin particionar en exceso |
| `OPTIMIZE` + `ANALYZE TABLE ... COMPUTE STATISTICS` | `gold.fact_sales` | Compacta small files; estadísticas para el optimizador |
| Broadcast join de dimensiones | `build_marts.py` | Evita shuffle de la tabla de hechos |
| Mart pre-agregado `mart_daily_sales` | Gold | El tablero lee miles de filas, no millones |
| AQE + optimize write + auto compact | Config del clúster en `databricks.yml` | Particiones balanceadas, menos archivos pequeños |
| Tipos compactos (`decimal`, `int`, `date_key`) | Silver/Gold | Menos I/O, joins por entero |

## Metodología

[benchmark.py](../src/lakehouse/optimization/benchmark.py) compara, sobre los mismos datos:

- **Baseline**: copia de `silver.transactions` en 400 archivos pequeños, sin clustering, con joins normales.
- **Optimizado**: tablas Gold con las técnicas anteriores.

Cada consulta se ejecuta 1 vez de calentamiento + 5 corridas medidas; se reporta la **mediana**. Se desactiva el disk cache para no sesgar. Los resultados se guardan en `governance.benchmark_results` y el porcentaje de mejora se calcula, no se escribe a mano.

Ejecutar: `benchmark` (entry point del paquete) o como tarea de job, con ≥ 1 M de filas en `silver.transactions`.

## Resultados

> **Pendiente de medición.** Este repositorio se desarrolló sin acceso a un workspace de Databricks, por lo que no hay números reales todavía. Tras ejecutar el benchmark, pegue aquí la salida:
>
> | Consulta | Baseline (s) | Optimizado (s) | Mejora |
> |---|---|---|---|
> | q1_revenue_by_region_day | | | |
> | q2_top_categories | | | |
> | q3_single_store_one_day | | | |
> | q4_customer_history | | | |
> | **Promedio** | | | |

Nota: las consultas q1 y q2 se benefician sobre todo del mart pre-agregado y q3/q4 del clustering; es esperable que la mejora varíe por consulta.
