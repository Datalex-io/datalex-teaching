# Phase 2 — Orchestration with Kestra (Correction)

## 🎯 Objectives

In this phase, you will orchestrate the **analytical batch pipeline** built in Phase 1 using **Kestra**.

You will:
- Orchestrate Python and SQL tasks
- Parameterize executions with `batch_date`
- Understand modern orchestration concepts
- Compare Kestra with Airflow, Dagster, and Prefect

---

## 🧱 Architecture Reminder

Pipeline orchestrated by Kestra:

1. Python extraction: **S3 / MinIO → PostgreSQL Staging**
2. SQL procedures:
   - Reference data seeding
   - Staging → Data Warehouse load
3. End‑to‑end execution using a single parameter

Kestra acts as the **control plane**, not a data store.

---

## 🛠️ Prerequisites

- Docker & Docker Compose
- Analytics PostgreSQL database (Phase 1)
- S3 or MinIO with CSV exports
- Local Git repository (Python & SQL scripts)

---

## 🐳 Run Kestra locally

```bash
docker compose up -d
```

Web UI:
👉 http://localhost:8081

---

## ▶️ Hands‑on Overview

You will orchestrate:

1. Python extraction:
   - `phase1_s3_to_staging.py`
2. Staging → DWH load
3. Stored procedures execution:
   - `dw.sp_seed_dim_feature`
   - `dw.sp_load_staging_to_dw`
4. With:
   - `batch_date = 2016-01-02`

---

## 🧩 Kestra Flow Logic

The flow:
- Takes `batch_date` as input
- Runs tasks sequentially
- Stops on failure
- Centralizes logs, retries, and status

---

## 🐳 Python Execution via Docker (recommended)

The Python task uses:

```yaml
containerImage: python:3.12-slim
```

Benefits:
- Dependency isolation
- Reproducible execution
- Production‑like behavior

⚠️ Requires:
```text
/var/run/docker.sock
```

---

## 🔁 Alternative: Local Python execution (no Docker socket)

If you do not want to mount the Docker socket:

### Principle
- Remove `containerImage`
- Python must be available on the worker
- Dependencies installed manually

### Comparison

| Docker | Local |
|------|------|
| Isolated | Simpler |
| Reproducible | Machine‑dependent |
| Production‑like | Training |

---

## 🔍 Observability

Kestra provides:
- Execution history
- Task‑level logs
- Error handling and retries

---

## ✅ Expected Result

After a successful run:
- Staging tables populated
- Dimensions and facts loaded
- Data consistent for the given `batch_date`

---

## 🧠 Key Takeaways

- Orchestration ≠ transformation
- Parameters enable backfills
- Kestra unifies Python, SQL, and infrastructure
