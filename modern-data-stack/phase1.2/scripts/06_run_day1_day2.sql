-- Run the pipeline
CALL dw.sp_load_staging_to_dw('2025-01-01');
CALL dw.sp_load_staging_to_dw('2025-01-02');

-- Inspect run status
SELECT * FROM dq.etl_run ORDER BY started_at DESC;

-- Quick DQ summary
SELECT reject_reason, reject_severity, COUNT(*)
FROM dq.rejected_rows
GROUP BY reject_reason, reject_severity
ORDER BY COUNT(*) DESC;

-- Fact counts by day
SELECT date_id, COUNT(*) AS usage_events
FROM dw.fact_feature_usage
GROUP BY date_id
ORDER BY date_id;

-- Users SCD2 history example
SELECT user_id, tenant_id, email, role, valid_from, valid_to, is_current
FROM dw.dim_user
ORDER BY user_id, valid_from;
