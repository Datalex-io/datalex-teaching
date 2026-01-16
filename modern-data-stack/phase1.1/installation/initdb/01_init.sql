CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.tenant (
  tenant_id UUID,
  name TEXT,
  plan TEXT,
  region TEXT,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.user (
  user_id UUID,
  tenant_id UUID,
  email TEXT,
  role TEXT,
  created_at TIMESTAMP,
  last_login_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.feature (
  feature_id TEXT,
  name TEXT,
  category TEXT,
  introduced_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.feature_usage (
  usage_id UUID,
  tenant_id UUID,
  user_id UUID,
  feature_id TEXT,
  used_at TIMESTAMP,
  duration_sec INT
);
