# Phase 3 — Intégration Airbyte (Correction)

Ce dépôt contient les **éléments de correction** pour la **Phase 3** du cours *Modern Data Stack*.

Dans cette phase, nous introduisons **Airbyte** pour ingérer un référentiel métier externe (Customer / CRM) provenant de **Google Drive**, puis l’intégrer dans le pipeline existant et le Data Warehouse.

---

## 🎯 Objectifs de la Phase 3

À la fin de cette phase, les apprenants doivent comprendre :

- Pourquoi Airbyte est introduit dans la stack data
- Comment ingérer des données externes sans écrire de connecteurs custom
- Comment Airbyte s’intègre avec PostgreSQL (staging) et le Data Warehouse
- Comment gérer les **types de données, la qualité et l’enrichissement**
- Comment faire évoluer une procédure stockée existante en toute sécurité

---

## 🧩 Contexte métier

Nous souhaitons enrichir notre modèle analytique avec des **informations client** provenant de l’extérieur de la plateforme SaaS :

- Référentiel CRM (HubSpot)
- Maintenu par les équipes métiers
- Exporté et partagé via **Google Drive**
- Ingesté à l’aide de **Airbyte**

Ce référentiel client est ensuite joint aux tenants existants.

---

## 📁 Structure du dépôt

```
.
├── 00_schemas_table.sql
├── 01_alter_dw.dim_tenant.sql
├── 02_rejected_and_enrich_dim_tenant.sql
├── 02_sp_load_staging_to_dw.sql
└── 03_validation.sql
```

---

## 📄 Description des fichiers

### `00_schemas_table.sql` (⚠️ informatif uniquement)

Ce fichier **n’est PAS exécuté**.

- Il montre la structure attendue de `staging.customer`
- Il sert de documentation et de référence pédagogique
- En pratique, **c’est Airbyte qui crée la table automatiquement**

---

### `01_alter_dw.dim_tenant.sql`

Ce script modifie la dimension du Data Warehouse :

- Ajout des attributs liés aux clients dans `dw.dim_tenant`
- La dimension reste en **SCD Type 1**
- Script exécuté **une seule fois**

---

### `02_rejected_and_enrich_dim_tenant.sql`

Ce fichier **n’est pas un script autonome**.

Il permet de :
- Montrer les nouvelles règles de rejet liées aux données clients
- Illustrer la logique de jointure pour enrichir `dim_tenant`
- Mettre en évidence les modifications apportées par l’introduction d’Airbyte

Objectif : **comprendre ce qui a changé et pourquoi**.

---

### `02_sp_load_staging_to_dw.sql`

Il s’agit de la **procédure stockée mise à jour**.

Elle inclut :
- Toute la logique précédente (dimensions, faits, SCD)
- Les nouveaux rejets liés aux clients
- Les **casts explicites de types** (`TEXT → UUID`, `TEXT → DATE`)
- L’enrichissement de `dw.dim_tenant` via `LEFT JOIN`

C’est le **livrable principal exécutable** de cette phase.

---

### `03_validation.sql`

Ce fichier contient des requêtes de validation et d’analyse :

- Nombre de tenants enrichis
- Tenants sans information client
- Analyses d’usage par attributs client

Il est utilisé pour :
- valider le bon fonctionnement du pipeline
- servir de support à l’exercice SQL final

---

## ⚠️ Points techniques importants

### 1. Airbyte charge les données externes en TEXT

Airbyte ingère très souvent les données externes sous forme de `TEXT` :
- identifiants
- dates
- valeurs numériques

➡️ Des **casts explicites** sont donc nécessaires dans SQL :
```sql
tenant_id::uuid
contract_start_date::date
```

Ce comportement est volontaire et reflète des pipelines réels.

---

### 2. Airbyte n’est pas un outil de transformation

- Airbyte : ingestion & connectivité
- SQL : qualité des données, modélisation, SCD
- Kestra : orchestration

Chaque outil a un rôle clair.

---

### 3. Le LEFT JOIN est indispensable

Les données clients sont un **enrichissement**, pas une source maîtresse.

Règles :
- Un tenant doit exister même sans donnée client
- Les référentiels externes ne doivent jamais supprimer des entités cœur

---

## ✅ Ordre d’exécution (Correction)

1. Synchronisation Airbyte → `staging.customers_staging`
2. Exécution de `01_alter_dw.dim_tenant.sql` (une seule fois)
3. Exécution de `02_sp_load_staging_to_dw.sql`
4. Exécution des requêtes de `03_validation.sql`

---

## 🧠 Points clés à retenir

- Airbyte permet de scaler l’ingestion sans écrire de connecteurs
- Les données métiers externes sont la norme
- Le typage et la qualité restent critiques
- Les procédures stockées évoluent de manière incrémentale