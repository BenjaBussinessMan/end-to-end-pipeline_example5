# Arquitectura

## 1. Visión end-to-end (Medallion)

```mermaid
flowchart LR
    subgraph SRC[Sistemas fuente aislados]
        POS[POS / Tiendas]
        CRM[CRM Clientes]
        ERP[ERP Productos]
    end

    subgraph UC[Unity Catalog - lakehouse_env]
        direction LR
        LAND[(landing<br/>Volumes: JSON)]
        BR[(bronze<br/>raw append-only)]
        SI[(silver<br/>limpio + SCD2)]
        QU[(silver.transactions_quarantine)]
        GO[(gold<br/>estrella + marts)]
        GV[(governance<br/>dq_results, benchmark_results)]
    end

    POS --> LAND
    CRM --> LAND
    ERP --> LAND
    LAND -- Auto Loader --> BR
    BR -- "tipado, dedupe, DQ, MERGE" --> SI
    SI -. reglas fallidas .-> QU
    SI -. métricas .-> GV
    SI -- "joins broadcast, agregados" --> GO
    GO --> SQL[Databricks SQL Warehouse]
    SQL --> PBI[Power BI<br/>Tablero ejecutivo]
    GO --> DS[Data Science / ML]
```

## 2. Orquestación (Databricks Workflow)

```mermaid
flowchart LR
    B[bronze<br/>Auto Loader] --> SD[silver_dims<br/>SCD2]
    B --> ST[silver_transactions<br/>MERGE + DQ]
    SD --> G[gold<br/>fact + dims + mart]
    ST --> G
    G --> Q{dq_gate}
    Q -- OK --> DONE([Publicado a BI])
    Q -- falla --> ALERT([Alerta por correo])
```

## 3. Secuencia de una corrida incremental

```mermaid
sequenceDiagram
    participant J as Workflow (02:00)
    participant L as Landing
    participant B as Bronze
    participant S as Silver
    participant G as Gold
    participant D as governance.dq_results
    J->>L: Detecta archivos nuevos (checkpoint)
    L->>B: Append + _ingest_ts, _source_file
    J->>S: Lee filas con _ingest_ts > high-water mark
    S->>S: Tipar, deduplicar, validar reglas
    S->>D: Métricas por regla
    S->>S: MERGE válidas / append cuarentena
    J->>G: Reconstruye fact, dims y mart_daily_sales
    J->>D: dq_gate (umbral de fallos + frescura)
```

## 4. Modelo dimensional (Gold)

```mermaid
erDiagram
    FACT_SALES }o--|| DIM_DATE : date_key
    FACT_SALES }o--|| DIM_CUSTOMER : customer_id
    FACT_SALES }o--|| DIM_PRODUCT : product_id
    FACT_SALES }o--|| DIM_STORE : store_id
    MART_DAILY_SALES }o--|| DIM_DATE : date_key

    FACT_SALES {
        string transaction_id PK
        int date_key FK
        long customer_id FK
        long product_id FK
        long store_id FK
        decimal net_amount
        decimal cost_amount
        decimal margin_amount
        int quantity
    }
    DIM_CUSTOMER {
        long customer_id PK
        string full_name "PII"
        string email "PII"
        string region
        string segment
    }
    DIM_PRODUCT {
        long product_id PK
        string category
        double unit_cost
    }
    DIM_STORE {
        long store_id PK
        string region
    }
    DIM_DATE {
        int date_key PK
        date date
        int year
        int month
    }
    MART_DAILY_SALES {
        int date_key
        string region
        string category
        decimal revenue
        decimal margin
        long transactions
    }
```

## 5. Decisiones de diseño

| Decisión | Motivo |
|---|---|
| Auto Loader con `availableNow` | Ingesta incremental con checkpoint, sin clúster 24/7 |
| Bronze sin transformar | Reprocesable y auditable; la fuente de verdad cruda |
| `MERGE` por `transaction_id` con condición `_ingest_ts` | Idempotencia: reejecutar no duplica ni retrocede datos |
| Cuarentena en vez de descartar | Los datos defectuosos se investigan y se pueden reinyectar |
| SCD2 solo en clientes | Es la dimensión cuyo histórico (región/segmento) afecta el análisis |
| Liquid Clustering `(event_date, store_id)` | Reemplaza particionado + ZORDER; evita small files y es adaptable |
| Mart pre-agregado | El tablero consulta miles de filas, no millones |
