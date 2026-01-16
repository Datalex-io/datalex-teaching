CREATE TABLE IF NOT EXISTS dq.etl_run (
  run_id UUID PRIMARY KEY,
  pipeline_name TEXT NOT NULL,
  batch_date DATE NOT NULL,
  started_at TIMESTAMP NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMP,
  status TEXT NOT NULL DEFAULT 'running', -- running|success|failed
  comment TEXT
);

CREATE TABLE IF NOT EXISTS dq.audit_checks (
  run_id UUID NOT NULL REFERENCES dq.etl_run(run_id),
  check_name TEXT NOT NULL,
  object_name TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  metric_value BIGINT NOT NULL,
  passed BOOLEAN,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Solution A: single generic rejects table
CREATE TABLE IF NOT EXISTS dq.rejected_rows (
  run_id UUID NOT NULL REFERENCES dq.etl_run(run_id),
  source_table TEXT NOT NULL,
  reject_reason TEXT NOT NULL,
  reject_severity TEXT NOT NULL DEFAULT 'error', -- warn|error
  record_pk TEXT,
  record_payload JSONB NOT NULL,
  rejected_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rejected_rows_run ON dq.rejected_rows(run_id);
CREATE INDEX IF NOT EXISTS idx_audit_checks_run ON dq.audit_checks(run_id);
