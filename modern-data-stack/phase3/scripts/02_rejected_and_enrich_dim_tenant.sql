-- Reject NULL tenant_id (error)
INSERT INTO dq.rejected_rows (
  run_id, source_table, reject_reason, reject_severity, record_pk, record_payload
)
SELECT
  v_run_id,
  'staging.customer',
  'NULL_TENANT_ID',
  'error',
  NULL,
  to_jsonb(c)
FROM staging.customer c
WHERE c.tenant_id IS NULL;


-- Reject duplicate tenant_id (warn)
WITH ranked AS (
  SELECT
    c.*,
    ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY customer_name NULLS LAST) AS rn
  FROM staging.customer c
  WHERE tenant_id IS NOT NULL
),
dups AS (
  SELECT * FROM ranked WHERE rn > 1
)
INSERT INTO dq.rejected_rows (
  run_id, source_table, reject_reason, reject_severity, record_pk, record_payload
)
SELECT
  v_run_id,
  'staging.customer',
  'DUPLICATE_TENANT_ID_IN_CUSTOMERS',
  'warn',
  tenant_id::text,
  to_jsonb(dups)
FROM dups;

-- Customer for unknown tenant
INSERT INTO dq.rejected_rows (
  run_id, source_table, reject_reason, reject_severity, record_pk, record_payload
)
SELECT
  v_run_id,
  'staging.customer',
  'CUSTOMER_FOR_UNKNOWN_TENANT',
  'warn',
  c.tenant_id::text,
  to_jsonb(c)
FROM staging.customer c
LEFT JOIN staging.tenant t
  ON t.tenant_id = c.tenant_id
WHERE c.tenant_id IS NOT NULL
  AND t.tenant_id IS NULL;



  -- SCD1 UPSERT final (clean & lisible)
WITH latest_tenant AS (
  SELECT *
  FROM (
    SELECT
      t.*,
      ROW_NUMBER() OVER (PARTITION BY t.tenant_id ORDER BY t.created_at DESC NULLS LAST) AS rn
    FROM staging.tenant t
    WHERE t.tenant_id IS NOT NULL
  ) x
  WHERE rn = 1
),
latest_customer AS (
  SELECT *
  FROM (
    SELECT
      c.*,
      ROW_NUMBER() OVER (PARTITION BY c.tenant_id ORDER BY c.customer_name NULLS LAST) AS rn
    FROM staging.customer c
    WHERE c.tenant_id IS NOT NULL
  ) y
  WHERE rn = 1
)
INSERT INTO dw.dim_tenant (
  tenant_id,
  name,
  plan,
  region,
  created_at,
  updated_at,
  customer_name,
  customer_type,
  country,
  industry,
  contract_start_date,
  account_owner
)
SELECT
  t.tenant_id,
  t.name,
  t.plan,
  t.region,
  t.created_at,
  NOW(),
  c.customer_name,
  c.customer_type,
  c.country,
  c.industry,
  c.contract_start_date,
  c.account_owner
FROM latest_tenant t
LEFT JOIN latest_customer c
  ON c.tenant_id = t.tenant_id
ON CONFLICT (tenant_id) DO UPDATE SET
  name = EXCLUDED.name,
  plan = EXCLUDED.plan,
  region = EXCLUDED.region,
  created_at = EXCLUDED.created_at,
  updated_at = NOW(),
  customer_name = EXCLUDED.customer_name,
  customer_type = EXCLUDED.customer_type,
  country = EXCLUDED.country,
  industry = EXCLUDED.industry,
  contract_start_date = EXCLUDED.contract_start_date,
  account_owner = EXCLUDED.account_owner;