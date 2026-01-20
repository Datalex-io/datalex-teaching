CREATE OR REPLACE PROCEDURE dw.sp_load_staging_to_dw(p_batch_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
  v_run_id UUID := gen_random_uuid();
  v_pipeline TEXT := 'phase1_2_staging_to_dw';
  v_rows BIGINT;
  v_rejected BIGINT;
  v_min_date DATE;
  v_max_date DATE;
BEGIN
  INSERT INTO dq.etl_run(run_id, pipeline_name, batch_date, status)
  VALUES (v_run_id, v_pipeline, p_batch_date, 'running');

  -------------------------------------------------------------------
  -- Audit staging counts
  -------------------------------------------------------------------
  SELECT COUNT(*) INTO v_rows FROM staging.tenant;
  INSERT INTO dq.audit_checks VALUES (v_run_id, 'staging_count', 'staging.tenant', 'rows_in_staging', v_rows, NULL, NOW());

  SELECT COUNT(*) INTO v_rows FROM staging.user;
  INSERT INTO dq.audit_checks VALUES (v_run_id, 'staging_count', 'staging.user', 'rows_in_staging', v_rows, NULL, NOW());

  SELECT COUNT(*) INTO v_rows FROM staging.feature_usage;
  INSERT INTO dq.audit_checks VALUES (v_run_id, 'staging_count', 'staging.feature', 'rows_in_staging', v_rows, NULL, NOW());

  -------------------------------------------------------------------
  -- DIM TENANT (SCD1)
  -------------------------------------------------------------------

  -- Reject null tenant_id
  INSERT INTO dq.rejected_rows(run_id, source_table, reject_reason, reject_severity, record_pk, record_payload)
  SELECT v_run_id, 'staging.tenant', 'NULL_TENANT_ID', 'error', NULL, to_jsonb(t)
  FROM staging.tenant t
  WHERE t.tenant_id IS NULL;

  -- Reject duplicate tenant_id (warn) - keep latest
  WITH ranked AS (
    SELECT
      t.*,
      ROW_NUMBER() OVER (PARTITION BY t.tenant_id ORDER BY t.created_at DESC NULLS LAST) AS rn
    FROM staging.tenant t
    WHERE t.tenant_id IS NOT NULL
  ),
  dups AS (
    SELECT * FROM ranked WHERE rn > 1
  )
  INSERT INTO dq.rejected_rows(run_id, source_table, reject_reason, reject_severity, record_pk, record_payload)
  SELECT v_run_id, 'staging.tenant', 'DUPLICATE_TENANT_ID', 'warn', tenant_id::TEXT, to_jsonb(dups)
  FROM dups;

  -- Upsert latest tenant into dim_tenant
  WITH latest AS (
    SELECT *
    FROM (
      SELECT
        t.*,
        ROW_NUMBER() OVER (PARTITION BY t.tenant_id ORDER BY t.created_at DESC NULLS LAST) AS rn
      FROM staging.tenant t
      WHERE t.tenant_id IS NOT NULL
    ) x
    WHERE rn = 1
  )
  INSERT INTO dw.dim_tenant(tenant_id, name, plan, region, created_at, updated_at)
  SELECT tenant_id, name, plan, region, created_at, NOW()
  FROM latest
  ON CONFLICT (tenant_id) DO UPDATE SET
    name = EXCLUDED.name,
    plan = EXCLUDED.plan,
    region = EXCLUDED.region,
    created_at = EXCLUDED.created_at,
    updated_at = NOW();

  -- Get the count of affected rows
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  INSERT INTO dq.audit_checks VALUES (v_run_id, 'load_dim', 'dw.dim_tenant', 'rows_upserted', v_rows, NULL, NOW());

  -------------------------------------------------------------------
  -- DIM USER (SCD2 on tenant_id + email + role)
  -- created_at / last_login_at are update-only on current version
  -------------------------------------------------------------------

  -- Reject NULL user_id
  INSERT INTO dq.rejected_rows(run_id, source_table, reject_reason, reject_severity, record_pk, record_payload)
  SELECT v_run_id, 'staging.user', 'NULL_USER_ID', 'error', NULL, to_jsonb(u)
  FROM staging.user u
  WHERE u.user_id IS NULL;

  -- Reject duplicates in staging (warn) - keep best record, reject others
  WITH ranked AS (
    SELECT
      u.*,
      ROW_NUMBER() OVER (
        PARTITION BY u.user_id
        ORDER BY u.last_login_at DESC NULLS LAST, u.created_at DESC NULLS LAST
      ) AS rn
    FROM staging.user u
    WHERE u.user_id IS NOT NULL
  ),
  dups AS (
    SELECT * FROM ranked WHERE rn > 1
  )
  INSERT INTO dq.rejected_rows(run_id, source_table, reject_reason, reject_severity, record_pk, record_payload)
  SELECT v_run_id, 'staging.user', 'DUPLICATE_USER_ID', 'warn', user_id::TEXT, to_jsonb(dups)
  FROM dups;

  -- Reject users referencing unknown tenant_id (error)
  INSERT INTO dq.rejected_rows(run_id, source_table, reject_reason, reject_severity, record_pk, record_payload)
  SELECT v_run_id, 'staging.user', 'UNKNOWN_TENANT_ID', 'error', u.user_id::TEXT, to_jsonb(u)
  FROM staging.user u
  LEFT JOIN dw.dim_tenant dt ON dt.tenant_id = u.tenant_id
  WHERE u.user_id IS NOT NULL
    AND u.tenant_id IS NOT NULL
    AND dt.tenant_id IS NULL;

  -- Build the "latest" row per user_id for this batch and compute the SCD2 hash
  -- Hash tracks: tenant_id + email + role
  CREATE TEMP TABLE tmp_user_latest ON COMMIT DROP AS
  SELECT
    x.user_id,
    x.tenant_id,
    x.email,
    COALESCE(NULLIF(x.role,''), 'unknown') AS role,
    x.created_at,
    x.last_login_at,
    md5(
      COALESCE(x.tenant_id::TEXT,'') || '|' ||
      COALESCE(x.email,'') || '|' ||
      COALESCE(COALESCE(NULLIF(x.role,''), 'unknown'),'unknown')
    ) AS scd_hash
  FROM (
    SELECT
      u.*,
      ROW_NUMBER() OVER (
        PARTITION BY u.user_id
        ORDER BY u.last_login_at DESC NULLS LAST, u.created_at DESC NULLS LAST
      ) AS rn
    FROM staging.user u
    WHERE u.user_id IS NOT NULL
  ) x
  WHERE x.rn = 1;

  -- Exclude rows with unknown tenant_id from SCD2 loading candidates
  -- (they are already logged in dq.rejected_rows)
  DELETE FROM tmp_user_latest l
  WHERE l.tenant_id IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM dw.dim_tenant dt
      WHERE dt.tenant_id = l.tenant_id
    );

  -- Current "active" version in DW
  CREATE TEMP TABLE tmp_user_current ON COMMIT DROP AS
  SELECT *
  FROM dw.dim_user
  WHERE is_current = TRUE;

  -- Split into NEW / CHANGED / UNCHANGED sets
  CREATE TEMP TABLE tmp_user_new ON COMMIT DROP AS
  SELECT l.*
  FROM tmp_user_latest l
  LEFT JOIN tmp_user_current c ON c.user_id = l.user_id
  WHERE c.user_id IS NULL;

  CREATE TEMP TABLE tmp_user_changed ON COMMIT DROP AS
  SELECT l.*, c.user_sk AS current_user_sk
  FROM tmp_user_latest l
  JOIN tmp_user_current c ON c.user_id = l.user_id
  WHERE c.scd_hash <> l.scd_hash;

  CREATE TEMP TABLE tmp_user_unchanged ON COMMIT DROP AS
  SELECT l.*, c.user_sk AS current_user_sk
  FROM tmp_user_latest l
  JOIN tmp_user_current c ON c.user_id = l.user_id
  WHERE c.scd_hash = l.scd_hash;

  -- Expire old versions for CHANGED users
  UPDATE dw.dim_user d
  SET
    valid_to = p_batch_date - INTERVAL '1 day',
    is_current = FALSE
  FROM tmp_user_changed ch
  WHERE d.user_sk = ch.current_user_sk;

  -- Insert new versions for CHANGED users
  INSERT INTO dw.dim_user(
    user_id, tenant_id, email, role,
    created_at, last_login_at,
    valid_from, valid_to, is_current,
    scd_hash
  )
  SELECT
    user_id, tenant_id, email, role,
    created_at, last_login_at,
    p_batch_date, NULL, TRUE,
    scd_hash
  FROM tmp_user_changed;

  -- Insert brand NEW users
  INSERT INTO dw.dim_user(
    user_id, tenant_id, email, role,
    created_at, last_login_at,
    valid_from, valid_to, is_current,
    scd_hash
  )
  SELECT
    user_id, tenant_id, email, role,
    created_at, last_login_at,
    p_batch_date, NULL, TRUE,
    scd_hash
  FROM tmp_user_new;

  -- Update-only fields for UNCHANGED users (no new version)
  UPDATE dw.dim_user d
  SET
    created_at = COALESCE(d.created_at, u.created_at),
    last_login_at = GREATEST(d.last_login_at, u.last_login_at)
  FROM tmp_user_unchanged u
  WHERE d.user_sk = u.current_user_sk;


  GET DIAGNOSTICS v_rows = ROW_COUNT;
  INSERT INTO dq.audit_checks VALUES (v_run_id, 'load_dim', 'dw.dim_user', 'rows_inserted_or_updated', v_rows, NULL, NOW());

  -------------------------------------------------------------------
  -- DIM DATE: build based on actual used_at dates in staging
  -------------------------------------------------------------------
  SELECT MIN(used_at::date), MAX(used_at::date)
  INTO v_min_date, v_max_date
  FROM staging.feature_usage
  WHERE used_at IS NOT NULL;

  IF v_min_date IS NOT NULL AND v_max_date IS NOT NULL THEN
    CALL dw.sp_build_dim_date(v_min_date, v_max_date);
  END IF;

  -------------------------------------------------------------------
  -- FACT FEATURE USAGE (only SKs + date_id)
  -------------------------------------------------------------------

  -- Reject duplicate usage_id in staging (warn)
  WITH ranked AS (
    SELECT
      fu.*,
      ROW_NUMBER() OVER (PARTITION BY fu.usage_id ORDER BY fu.used_at DESC NULLS LAST) AS rn
    FROM staging.feature_usage fu
    WHERE fu.usage_id IS NOT NULL
  ),
  dups AS (
    SELECT * FROM ranked WHERE rn > 1
  )
  INSERT INTO dq.rejected_rows(run_id, source_table, reject_reason, reject_severity, record_pk, record_payload)
  SELECT v_run_id, 'staging.feature_usage', 'DUPLICATE_USAGE_ID', 'warn', usage_id::TEXT, to_jsonb(dups)
  FROM dups;

  -- Reject basic invalid facts (error)
  INSERT INTO dq.rejected_rows(run_id, source_table, reject_reason, reject_severity, record_pk, record_payload)
  SELECT v_run_id, 'staging.feature_usage', 'INVALID_FACT_BASIC', 'error', COALESCE(fu.usage_id::TEXT,'(null)'), to_jsonb(fu)
  FROM staging.feature_usage fu
  WHERE fu.usage_id IS NULL
     OR fu.tenant_id IS NULL
     OR fu.user_id IS NULL
     OR fu.feature_id IS NULL OR fu.feature_id = ''
     OR fu.used_at IS NULL;

  -- Reject basic NEGATIVE_DURATION (error)
  INSERT INTO dq.rejected_rows(run_id, source_table, reject_reason, reject_severity, record_pk, record_payload)
  SELECT v_run_id, 'staging.feature_usage', 'NEGATIVE_DURATION', 'error', COALESCE(fu.usage_id::TEXT,'(null)'), to_jsonb(fu)
  FROM staging.feature_usage fu
  WHERE fu.duration_sec < 0;

  -- Optional: reject unknown feature_id (reference data)
  INSERT INTO dq.rejected_rows(run_id, source_table, reject_reason, reject_severity, record_pk, record_payload)
  SELECT v_run_id, 'staging.feature_usage', 'UNKNOWN_FEATURE_ID', 'error', fu.usage_id::TEXT, to_jsonb(fu)
  FROM staging.feature_usage fu
  LEFT JOIN dw.dim_feature df ON df.feature_id = fu.feature_id
  WHERE fu.usage_id IS NOT NULL
    AND fu.feature_id IS NOT NULL AND fu.feature_id <> ''
    AND df.feature_id IS NULL;

  -- Optional: reject if no SCD2 user match (covers missing user or gap in validity range)
  -- We'll compute during join and reject those rows.

  WITH candidates AS (
    SELECT fu.*
    FROM staging.feature_usage fu
    WHERE fu.usage_id IS NOT NULL
      AND fu.tenant_id IS NOT NULL
      AND fu.user_id IS NOT NULL
      AND fu.feature_id IS NOT NULL AND fu.feature_id <> ''
      AND fu.used_at IS NOT NULL
      AND fu.duration_sec >= 0
  ),
  with_tenant AS (
    SELECT c.*, dt.tenant_sk
    FROM candidates c
    JOIN dw.dim_tenant dt ON dt.tenant_id = c.tenant_id
  ),
  with_feature AS (
    SELECT wt.*, df.feature_sk
    FROM with_tenant wt
    JOIN dw.dim_feature df ON df.feature_id = wt.feature_id
  ),
  with_user AS (
    SELECT
      wf.*,
      du.user_sk
    FROM with_feature wf
    JOIN dw.dim_user du
      ON du.user_id = wf.user_id
     AND wf.used_at::date >= du.valid_from
     AND (du.valid_to IS NULL OR wf.used_at::date <= du.valid_to)
  ),
  dedup AS (
    SELECT
      wu.*,
      ROW_NUMBER() OVER (PARTITION BY usage_id ORDER BY used_at DESC NULLS LAST) AS rn
    FROM with_user wu
  )
  INSERT INTO dw.fact_feature_usage(usage_id, tenant_sk, user_sk, feature_sk, date_id, used_at, duration_sec)
  SELECT
    usage_id,
    tenant_sk,
    user_sk,
    feature_sk,
    (to_char(used_at::date, 'YYYYMMDD'))::INT AS date_id,
    used_at,
    duration_sec
  FROM dedup
  WHERE rn = 1
  ON CONFLICT (usage_id) DO UPDATE SET
    tenant_sk = EXCLUDED.tenant_sk,
    user_sk = EXCLUDED.user_sk,
    feature_sk = EXCLUDED.feature_sk,
    date_id = EXCLUDED.date_id,
    used_at = EXCLUDED.used_at,
    duration_sec = EXCLUDED.duration_sec;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  INSERT INTO dq.audit_checks VALUES (v_run_id, 'load_fact', 'dw.fact_feature_usage', 'rows_upserted', v_rows, NULL, NOW());

  -- Reject FK mismatches (tenant/feature/user/date)
  -- This runs once and clearly explains why some rows didn't load.
  WITH candidates AS (
    SELECT fu.*
    FROM staging.feature_usage fu
    WHERE fu.usage_id IS NOT NULL
      AND fu.tenant_id IS NOT NULL
      AND fu.user_id IS NOT NULL
      AND fu.feature_id IS NOT NULL AND fu.feature_id <> ''
      AND fu.used_at IS NOT NULL
      AND fu.duration_sec >= 0
  ),
  t AS (
    SELECT c.*, dt.tenant_sk
    FROM candidates c
    LEFT JOIN dw.dim_tenant dt ON dt.tenant_id = c.tenant_id
  ),
  tf AS (
    SELECT t.*, df.feature_sk
    FROM t
    LEFT JOIN dw.dim_feature df ON df.feature_id = t.feature_id
  ),
  tfu AS (
    SELECT
      tf.*,
      du.user_sk
    FROM tf
    LEFT JOIN dw.dim_user du
      ON du.user_id = tf.user_id
     AND tf.used_at::date >= du.valid_from
     AND (du.valid_to IS NULL OR tf.used_at::date <= du.valid_to)
  ),
  tfud AS (
    SELECT
      tfu.*,
      dd.date_id AS date_exists
    FROM tfu
    LEFT JOIN dw.dim_date dd
      ON dd.date_id = (to_char(tfu.used_at::date, 'YYYYMMDD'))::INT
  )
  INSERT INTO dq.rejected_rows(run_id, source_table, reject_reason, reject_severity, record_pk, record_payload)
  SELECT
    v_run_id,
    'staging.feature_usage',
    CASE
      WHEN tenant_sk IS NULL THEN 'FK_TENANT_NOT_FOUND'
      WHEN feature_sk IS NULL THEN 'FK_FEATURE_NOT_FOUND'
      WHEN user_sk IS NULL THEN 'FK_USER_NOT_FOUND_OR_NO_SCD2_MATCH'
      WHEN date_exists IS NULL THEN 'FK_DATE_NOT_FOUND'
      ELSE 'UNKNOWN_FK_ERROR'
    END,
    'error',
    usage_id::TEXT,
    to_jsonb(tfud)
  FROM tfud
  WHERE tenant_sk IS NULL OR feature_sk IS NULL OR user_sk IS NULL OR date_exists IS NULL;

  -------------------------------------------------------------------
  -- Final audit
  -------------------------------------------------------------------
  SELECT COUNT(*) INTO v_rejected FROM dq.rejected_rows WHERE run_id = v_run_id;
  INSERT INTO dq.audit_checks VALUES (v_run_id, 'dq_summary', 'dq.rejected_rows', 'rows_rejected', v_rejected, (v_rejected = 0), NOW());

  UPDATE dq.etl_run
  SET finished_at = NOW(), status = 'success'
  WHERE run_id = v_run_id;

EXCEPTION WHEN OTHERS THEN
  UPDATE dq.etl_run
  SET finished_at = NOW(), status = 'failed', comment = SQLERRM
  WHERE run_id = v_run_id;
  RAISE;
END;
$$;