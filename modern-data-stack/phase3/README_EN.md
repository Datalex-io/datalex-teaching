# Phase 3 — Airbyte Integration (Correction)

This repository contains the **correction assets** for **Phase 3** of the *Modern Data Stack* course.

In this phase, we introduce **Airbyte** to ingest an external business reference (Customer / CRM) coming from **Google Drive**, and we integrate it into our existing pipeline and Data Warehouse.

---

## 🎯 Objectives of Phase 3

By the end of this phase, students should understand:

- Why Airbyte is introduced in the data stack
- How to ingest external reference data without custom code
- How Airbyte fits with PostgreSQL staging and the Data Warehouse
- How to handle **data types, data quality, and enrichment**
- How to evolve an existing stored procedure safely

---

## 🧩 Business Context

We want to enrich our analytical model with **customer information** coming from outside the SaaS platform:

- CRM reference (HubSpot)
- Maintained by business teams
- Exported and shared via **Google Drive**
- Ingested using **Airbyte**

This customer reference is joined with existing tenants.

---

## 📁 Repository Structure

```
.
├── 00_schemas_table.sql
├── 01_alter_dw.dim_tenant.sql
├── 02_rejected_and_enrich_dim_tenant.sql
├── 02_sp_load_staging_to_dw.sql
└── 03_validation.sql
```

---

## 📄 File Explanations

### `00_schemas_table.sql` (informational only)

This file **is NOT executed**.

- Shows the expected structure of `staging.customer`
- Provided for documentation purposes
- In reality, **Airbyte creates the table automatically**

---

### `01_alter_dw.dim_tenant.sql`

- Adds customer-related attributes to `dw.dim_tenant`
- Keeps the dimension in **SCD Type 1**
- Executed once

---

### `02_rejected_and_enrich_dim_tenant.sql`

This file is **not a standalone script**.

It documents:
- New reject rules for customer data
- Join logic to enrich `dim_tenant`
- Changes introduced by Airbyte

---

### `02_sp_load_staging_to_dw.sql`

This is the **updated stored procedure**.

It includes:
- All previous logic (dimensions, facts, SCDs)
- Customer-related rejects
- Explicit type casts (`TEXT → UUID`, `TEXT → DATE`)
- Enrichment of `dw.dim_tenant`

This is the main executable artifact of the phase.

---

### `03_validation.sql`

Contains validation and analysis queries:
- Tenants enriched vs total
- Tenants missing customer reference
- Usage analysis by customer attributes

---

## ⚠️ Important Technical Notes

### Airbyte loads external data as TEXT

Explicit casts are required in SQL:
- `tenant_id::uuid`
- `contract_start_date::date`

This reflects real-world ingestion behavior.

---

### Airbyte is an ingestion tool

- Airbyte: ingestion & connectivity
- SQL: data quality, modeling, SCD logic
- Kestra: orchestration

---

### LEFT JOIN is mandatory

Customer data is enrichment only.
Tenants must exist even without customer information.

---

## ✅ Execution Order

1. Airbyte sync → `staging.customers_staging`
2. Execute `01_alter_dw.dim_tenant.sql`
3. Execute `02_sp_load_staging_to_dw.sql`
4. Run `03_validation.sql`

---

## 🧠 Key Takeaways

- Airbyte scales ingestion without custom connectors
- External business data is common
- Data typing and quality remain critical
- Stored procedures evolve incrementally