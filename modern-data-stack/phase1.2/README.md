# Phase 1.2 — Staging → Data Warehouse (Correction)

Ce dépôt contient les **éléments de correction** pour la **Phase 1.2** du cours *Modern Data Stack*.

Cette phase correspond au passage **du staging vers le Data Warehouse**, entièrement réalisé en **SQL**, dans PostgreSQL, sans outil externe.

---

## 🎯 Objectifs de la Phase 1.2

À l’issue de cette phase, les apprenants doivent être capables de :

- Comprendre la différence entre **staging** et **Data Warehouse**
- Mettre en place une **architecture en étoile**
- Implémenter des **dimensions et des tables de faits**
- Gérer la **qualité des données** (rejects, doublons, clés invalides)
- Implémenter des **SCD (Slowly Changing Dimensions)**
- Centraliser la logique métier dans des **procédures stockées**

---

## 🧩 Contexte pédagogique

Les données issues de la plateforme SaaS ont été chargées dans le **staging PostgreSQL** via des exports CSV.

La mission de cette phase est de :
- fiabiliser les données,
- les transformer,
- les historiser si nécessaire,
- et les charger dans un **Data Warehouse analytique**.

Aucun outil externe n’est utilisé ici : **PostgreSQL est suffisant**.

---

## 📁 Structure du dépôt

```text
.
├── 00_extensions_and_schemas.sql
├── 01_dq_tables.sql
├── 02_dw_tables.sql
├── 03_seed_dim_feature.sql
├── 04_sp_build_dim_date.sql
├── 05_sp_load_staging_to_dw.sql
└── 06_run_day1_day2.sql
```

---

## 📄 Description des fichiers

### `00_extensions_and_schemas.sql`

- Active les extensions PostgreSQL nécessaires
- Crée les schémas logiques :
  - `staging`
  - `dw`
  - `dq`

Ce script est exécuté **une seule fois**.

---

### `01_dq_tables.sql`

- Crée les tables de **Data Quality**
- Centralise les rejets :
  - lignes invalides
  - erreurs de clés
  - doublons
- Permet l’audit et la traçabilité

Toutes les phases suivantes s’appuient sur ces tables.

---

### `02_dw_tables.sql`

- Crée les tables du **Data Warehouse**
- Modèle en étoile :
  - dimensions (`dim_tenant`, `dim_user`, `dim_feature`, `dim_date`)
  - faits (`fact_feature_usage`, etc.)
- Utilisation de **surrogate keys** (`BIGINT`)

---

### `03_seed_dim_feature.sql`

- Alimente la dimension **référentielle** `dim_feature`
- Les features sont **connues à l’avance**
- Toute divergence côté staging est considérée comme une erreur de données

Ce script simule un **référentiel métier stable**.

---

### `04_sp_build_dim_date.sql`

- Génère automatiquement la **dimension date**
- Une ligne par jour
- `date_sk = date_id`
- Dimension indispensable pour toute analyse temporelle

Script exécuté **avant le chargement des faits**.

---

### `05_sp_load_staging_to_dw.sql`

C’est le **cœur de la Phase 1.2**.

Cette procédure :
- charge les dimensions depuis le staging
- applique les règles de Data Quality
- implémente les SCD :
  - SCD1 pour `tenant`
  - SCD2 pour `user`
- charge les tables de faits
- rejette les données invalides dans `dq.rejected_rows`

Toute la logique métier est centralisée ici.

---

### `06_run_day1_day2.sql`

- Simule l’exécution du pipeline sur **plusieurs jours**
- Permet de tester :
  - les SCD
  - l’idempotence
  - la gestion des doublons
- Représente des exécutions batch successives

Très utile pour la compréhension du cycle de vie des données.

---

## ⚠️ Points techniques importants

### 1. Séparation staging / warehouse

- `staging` = données brutes / techniques
- `dw` = données fiables / analytiques
- aucune logique métier dans le staging

---

### 2. Surrogate keys

- Toutes les dimensions utilisent des `*_sk`
- Les tables de faits ne stockent **que des SK**
- Les identifiants métier restent dans les dimensions

---

### 3. Data Quality centralisée

- Les rejets ne bloquent pas le pipeline
- Ils sont tracés et auditables
- Une table générique est utilisée (`dq.rejected_rows`)

---

## ✅ Ordre d’exécution (Correction)

1. `00_extensions_and_schemas.sql`
2. `01_dq_tables.sql`
3. `02_dw_tables.sql`
4. `03_seed_dim_feature.sql`
5. `04_sp_build_dim_date.sql`
6. `05_sp_load_staging_to_dw.sql`
7. `06_run_day1_day2.sql`

---

## 🧠 Points clés à retenir

- Le Data Warehouse est une **couche logique**
- Le SQL reste central dans une Modern Data Stack
- La qualité des données est un **premier‑class citizen**
- Les SCD sont des patterns essentiels