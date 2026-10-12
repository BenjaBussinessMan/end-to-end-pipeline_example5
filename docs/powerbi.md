# Tablero ejecutivo en Power BI

El archivo `.pbix` es binario y se construye en Power BI Desktop; este documento y [measures.dax](../powerbi/measures.dax) contienen todo lo necesario para armarlo en pocos minutos.

## Conexión

1. Power BI Desktop → Obtener datos → **Databricks**.
2. Server hostname y HTTP path de un **SQL Warehouse** (Serverless recomendado).
3. Autenticación: Microsoft Entra ID / OAuth (cada usuario hereda sus permisos de Unity Catalog, incluidos masks y row filters) o un service principal del grupo `bi_readers` para refresco programado.
4. Catálogo `lakehouse_prod`, esquema `gold`. Importar: `dim_date`, `dim_customer`, `dim_product`, `dim_store`, `mart_daily_sales`. Usar **DirectQuery** para `fact_sales` si se requiere detalle; el resto en Import.

## Modelo semántico

| Relación | Cardinalidad | Filtro |
|---|---|---|
| `mart_daily_sales[date_key]` → `dim_date[date_key]` | N:1 | Único |
| `fact_sales[date_key]` → `dim_date[date_key]` | N:1 | Único |
| `fact_sales[customer_id]` → `dim_customer[customer_id]` | N:1 | Único |
| `fact_sales[product_id]` → `dim_product[product_id]` | N:1 | Único |
| `fact_sales[store_id]` → `dim_store[store_id]` | N:1 | Único |

Marcar `dim_date` como tabla de fechas. Ocultar las claves en la vista de informe.

## Páginas

```
┌──────────────────────────────────────────────────────────────┐
│ 1. RESUMEN EJECUTIVO                       [Año][Región]      │
│ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐                  │
│ │Ingresos│ │ Margen │ │ Ticket │ │ YoY %  │   (tarjetas)      │
│ └────────┘ └────────┘ └────────┘ └────────┘                  │
│ ┌───────────────────────────┐ ┌────────────────────────────┐ │
│ │ Ingresos por día (línea)  │ │ Ingresos por categoría     │ │
│ │ + mismo periodo año ant.  │ │ (barras horizontales)      │ │
│ └───────────────────────────┘ └────────────────────────────┘ │
│ ┌───────────────────────────────────────────────────────────┐ │
│ │ Ingresos por región (mapa de árbol o barras)               │ │
│ └───────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
2. VENTAS: matriz Región x Categoría, top 10 tiendas, detalle por cliente (PII enmascarada)
3. CALIDAD Y FRESCURA: % filas en cuarentena por regla (governance.dq_results),
   última actualización, tendencia de fallos, resultados del benchmark
```

## Buenas prácticas

- Medidas en una tabla `_Medidas` dedicada; sin columnas calculadas pesadas.
- Paleta daltónica-segura, un color por categoría coherente en todas las páginas.
- Refresco programado después del job (02:00 + margen); mostrar `Última actualización` en cada página.
- Row-level security: aplicada en Unity Catalog, no se duplica en Power BI.
