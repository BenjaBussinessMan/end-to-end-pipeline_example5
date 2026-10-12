-- Unity Catalog setup: catalog, schemas, volumes, RBAC. Run as metastore admin.
-- Replace ${env} with dev|prod (e.g. via a Databricks job parameter / widget).

CREATE CATALOG IF NOT EXISTS lakehouse_${env} COMMENT 'Enterprise lakehouse (Medallion)';
USE CATALOG lakehouse_${env};

CREATE SCHEMA IF NOT EXISTS landing    COMMENT 'Raw files from source systems';
CREATE SCHEMA IF NOT EXISTS bronze     COMMENT 'Raw, append-only Delta tables';
CREATE SCHEMA IF NOT EXISTS silver     COMMENT 'Cleansed, conformed, deduplicated';
CREATE SCHEMA IF NOT EXISTS gold       COMMENT 'Business-ready star schema and marts';
CREATE SCHEMA IF NOT EXISTS governance COMMENT 'DQ results, masking functions, audit helpers';

CREATE VOLUME IF NOT EXISTS landing.raw;
CREATE VOLUME IF NOT EXISTS landing._checkpoints;

CREATE TABLE IF NOT EXISTS governance.dq_results (
  rule STRING, failed_rows BIGINT, table_name STRING,
  total_rows BIGINT, failed_pct DOUBLE, run_ts TIMESTAMP
) COMMENT 'Data quality metrics per run and rule';

-- ---------------------------------------------------------------------
-- RBAC (least privilege). Groups are managed in the account / SCIM.
-- data_engineers : build everything          data_scientists : read silver+gold
-- data_analysts  : read gold + DQ results    bi_readers      : read gold only
-- ---------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG lakehouse_${env} TO `data_engineers`, `data_scientists`, `data_analysts`, `bi_readers`;

-- Engineers
GRANT ALL PRIVILEGES ON SCHEMA landing    TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA bronze     TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA silver     TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA gold       TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA governance TO `data_engineers`;

-- Data scientists: curated layers, read-only
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `data_scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `data_scientists`;

-- Analysts
GRANT USE SCHEMA, SELECT ON SCHEMA gold       TO `data_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA governance TO `data_analysts`;

-- BI service principal / readers
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `bi_readers`;

-- Bronze is never exposed to consumers (no grant = no access).

-- ---------------------------------------------------------------------
-- Metadata: comments and tags (discoverability + classification)
-- ---------------------------------------------------------------------
ALTER SCHEMA bronze SET TAGS ('layer' = 'bronze');
ALTER SCHEMA silver SET TAGS ('layer' = 'silver');
ALTER SCHEMA gold   SET TAGS ('layer' = 'gold');

ALTER TABLE silver.customers ALTER COLUMN email     SET TAGS ('classification' = 'pii');
ALTER TABLE silver.customers ALTER COLUMN full_name SET TAGS ('classification' = 'pii');
ALTER TABLE gold.dim_customer ALTER COLUMN email     SET TAGS ('classification' = 'pii');
ALTER TABLE gold.dim_customer ALTER COLUMN full_name SET TAGS ('classification' = 'pii');

-- ---------------------------------------------------------------------
-- Lineage and audit (system tables; requires system schema access enabled)
-- ---------------------------------------------------------------------
-- Upstream sources of a Gold table:
-- SELECT source_table_full_name, target_table_full_name, event_time
-- FROM system.access.table_lineage
-- WHERE target_table_full_name = 'lakehouse_${env}.gold.fact_sales';
--
-- Column-level lineage:
-- SELECT * FROM system.access.column_lineage
-- WHERE target_table_full_name = 'lakehouse_${env}.gold.fact_sales';
--
-- Who accessed PII tables in the last 7 days:
-- SELECT event_time, user_identity.email, action_name, request_params
-- FROM system.access.audit
-- WHERE event_date >= current_date() - 7 AND request_params['full_name_arg'] LIKE '%dim_customer%';
