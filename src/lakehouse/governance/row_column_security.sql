-- Fine-grained access control: column masks (PII) and row filters (region).
USE CATALOG lakehouse_${env};

-- Column mask: only members of `pii_readers` see the real value.
CREATE OR REPLACE FUNCTION governance.mask_email(email STRING)
RETURN CASE
  WHEN is_account_group_member('pii_readers') THEN email
  ELSE regexp_replace(email, '^[^@]+', '*****')
END;

CREATE OR REPLACE FUNCTION governance.mask_name(name STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN name ELSE '*** MASKED ***' END;

ALTER TABLE gold.dim_customer ALTER COLUMN email     SET MASK governance.mask_email;
ALTER TABLE gold.dim_customer ALTER COLUMN full_name SET MASK governance.mask_name;

-- Row filter: regional managers (group `region_<NAME>`) only see their region;
-- `data_engineers` and `all_regions` see everything.
CREATE OR REPLACE FUNCTION governance.region_filter(region STRING)
RETURN is_account_group_member('data_engineers')
    OR is_account_group_member('all_regions')
    OR is_account_group_member(concat('region_', region));

ALTER TABLE gold.dim_store     SET ROW FILTER governance.region_filter ON (region);
ALTER TABLE gold.mart_daily_sales SET ROW FILTER governance.region_filter ON (region);
