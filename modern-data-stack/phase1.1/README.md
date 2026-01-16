# Phase 1 — S3 → PostgreSQL Staging (Batch Ingestion)

This script loads daily CSV exports from S3 (or MinIO) into PostgreSQL **staging** tables, writes a **manifest** for traceability, and **archives** processed files.

## What you build

**Pattern:** `incoming → staging → manifest → archive`

- **Incoming:** immutable daily files
- **Staging:** raw tables (no data quality checks here)
- **Manifest:** a JSON trace of what was processed
- **Archive:** processed files moved out of `incoming/`

---

## S3 layout (required)

```
s3://<bucket>/saas-exports/
  incoming/
    batch_date=YYYY-MM-DD/
      full/
        tenants.csv
        users.csv
        features.csv
        support_tickets.csv
      incremental/
        feature_usage.csv

  manifests/
    batch_date=YYYY-MM-DD/
      run_id=<uuid>.json

  archive/
    batch_date=YYYY-MM-DD/
      run_id=<uuid>/
        full/
        incremental/
```

---

## PostgreSQL prerequisites

Create the staging schema and tables (run once):

```sql
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.tenants_staging (
  tenant_id UUID,
  name TEXT,
  plan TEXT,
  region TEXT,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.users_staging (
  user_id UUID,
  tenant_id UUID,
  email TEXT,
  role TEXT,
  created_at TIMESTAMP,
  last_login_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.features_staging (
  feature_id TEXT,
  name TEXT,
  category TEXT,
  introduced_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.support_tickets_staging (
  ticket_id UUID,
  tenant_id UUID,
  priority TEXT,
  status TEXT,
  opened_at TIMESTAMP,
  closed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.feature_usage_staging (
  usage_id UUID,
  tenant_id UUID,
  user_id UUID,
  feature_id TEXT,
  used_at TIMESTAMP,
  duration_sec INT
);
```

---

## Configuration (environment variables)

| Variable | Required | Example |
|---------|----------|---------|
| S3_BUCKET | yes | my-student-bucket |
| S3_PREFIX | no | saas-exports |
| BATCH_DATE | yes | 2025-01-01 |
| PG_DSN | yes | postgresql://etl:etl@localhost:5432/analytics |
| AWS_REGION | no | eu-west-1 |
| AWS_PROFILE | no | myprofile |
| S3_ENDPOINT_URL | no | http://localhost:9000 |
| S3_FORCE_PATH_STYLE | no | true |
| VERBOSE | no | true |

---

## Run on AWS (using a named profile)

```bash
unset S3_ENDPOINT_URL
unset S3_FORCE_PATH_STYLE
unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY

export AWS_PROFILE="myprofile" # if needed
export AWS_REGION="eu-west-1"

export S3_BUCKET="my-student-bucket"
export S3_PREFIX="saas-export"
export BATCH_DATE="2025-01-01"

export PG_DSN="postgresql://postgres:postgres@localhost:5432/eseo"

python phase1_s3_to_staging.py
```

---

## Run on MinIO

```bash
unset AWS_PROFILE # if needed

export S3_ENDPOINT_URL="http://localhost:9000"
export S3_FORCE_PATH_STYLE="true"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"

export S3_BUCKET="my-bucket"
export S3_PREFIX="saas-export"
export BATCH_DATE="2025-01-01"

export PG_DSN="postgresql://postgres:postgres@localhost:5432/eseo"

python phase1_s3_to_staging.py
```

---

## Output

After a successful run you should see:

- staging tables populated:
  - staging.tenants_staging
  - staging.users_staging
  - staging.features_staging
  - staging.support_tickets_staging
  - staging.feature_usage_staging

- manifest written to:
  - manifests/batch_date=YYYY-MM-DD/run_id=<uuid>.json

- files moved to:
  - archive/batch_date=YYYY-MM-DD/run_id=<uuid>/

---

## Next step

**Phase 1.2 — Staging → Data Warehouse**

You will:
- apply data quality rules
- store rejected rows
- write audit metrics
- load trusted tables into dw.*
