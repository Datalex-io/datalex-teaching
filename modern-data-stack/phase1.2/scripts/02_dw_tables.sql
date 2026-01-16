-- -----------------------------
-- Dimensions
-- -----------------------------

-- Tenant (SCD1)
CREATE TABLE IF NOT EXISTS dw.dim_tenant (
  tenant_sk BIGSERIAL PRIMARY KEY,
  tenant_id UUID UNIQUE NOT NULL,
  name TEXT,
  plan TEXT,
  region TEXT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Feature (reference data, seeded by teacher)
CREATE TABLE IF NOT EXISTS dw.dim_feature (
  feature_sk BIGSERIAL PRIMARY KEY,
  feature_id TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  introduced_at TIMESTAMP
);

-- Date dimension: date_id is the surrogate key (YYYYMMDD)
CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_id INT PRIMARY KEY,          -- YYYYMMDD
  date DATE UNIQUE NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL,
  iso_year INT NOT NULL,
  iso_week INT NOT NULL,
  day_of_week INT NOT NULL,         -- 1=Mon..7=Sun
  day_name TEXT NOT NULL,
  is_weekend BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_date_date ON dw.dim_date(date);

-- User (SCD2 on tenant_id + email + role)
CREATE TABLE IF NOT EXISTS dw.dim_user (
  user_sk BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL,                 
  tenant_id UUID,                        -- versioned (tenant change => new version)
  email TEXT,                            -- versioned
  role TEXT NOT NULL,                    -- versioned

  created_at TIMESTAMP,                  -- update-only
  last_login_at TIMESTAMP,               -- update-only

  valid_from DATE NOT NULL,
  valid_to DATE,
  is_current BOOLEAN NOT NULL DEFAULT TRUE,

  scd_hash TEXT NOT NULL,                -- md5(tenant_id|email|role)

  UNIQUE (user_id, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_dim_user_current_bk
  ON dw.dim_user(user_id) WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_dim_user_bk_range
  ON dw.dim_user(user_id, valid_from, valid_to);

-- -----------------------------
-- Fact
-- -----------------------------

CREATE TABLE IF NOT EXISTS dw.fact_feature_usage (
  usage_id UUID PRIMARY KEY,

  tenant_sk BIGINT NOT NULL REFERENCES dw.dim_tenant(tenant_sk),
  user_sk BIGINT NOT NULL REFERENCES dw.dim_user(user_sk),
  feature_sk BIGINT NOT NULL REFERENCES dw.dim_feature(feature_sk),
  date_id INT NOT NULL REFERENCES dw.dim_date(date_id),

  used_at TIMESTAMP NOT NULL,
  duration_sec INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_usage_date_id ON dw.fact_feature_usage(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_usage_tenant_sk ON dw.fact_feature_usage(tenant_sk);
CREATE INDEX IF NOT EXISTS idx_fact_usage_user_sk ON dw.fact_feature_usage(user_sk);
CREATE INDEX IF NOT EXISTS idx_fact_usage_feature_sk ON dw.fact_feature_usage(feature_sk);
