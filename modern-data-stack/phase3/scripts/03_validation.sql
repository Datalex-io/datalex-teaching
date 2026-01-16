-- Tenants enriched vs total
SELECT
  COUNT(*) AS total_tenants,
  COUNT(customer_name) AS tenants_with_customer
FROM dw.dim_tenant;

-- Missing customer reference
SELECT tenant_id, name, plan
FROM dw.dim_tenant
WHERE customer_name IS NULL;

-- Usage by customer_type
SELECT
  dt.customer_type,
  COUNT(*) AS usage_events
FROM dw.fact_feature_usage f
JOIN dw.dim_tenant dt
  ON dt.tenant_sk = f.tenant_sk
GROUP BY dt.customer_type
ORDER BY usage_events DESC;