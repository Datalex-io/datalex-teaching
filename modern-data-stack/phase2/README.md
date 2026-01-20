# Phase 2 — Orchestration avec Kestra (Correction)

## 🎯 Objectifs

Dans cette phase, vous allez orchestrer le **pipeline batch analytique** construit en Phase 1 à l’aide de **Kestra**.

Vous allez :
- Orchestrer des tâches Python et SQL
- Paramétrer les exécutions avec `batch_date`
- Comprendre les concepts modernes d’orchestration
- Comparer Kestra à Airflow, Dagster et Prefect

---

## 🧱 Rappel d’architecture

Pipeline orchestré par Kestra :

1. Extraction Python : **S3 / MinIO → PostgreSQL Staging**
2. Procédures SQL :
   - Seed des référentiels
   - Chargement Staging → Data Warehouse
3. Exécution bout‑en‑bout via un seul paramètre

Kestra est le **plan de contrôle**, pas un stockage de données.

---

## 🛠️ Prérequis

- Docker & Docker Compose
- Base PostgreSQL analytics (Phase 1)
- S3 ou MinIO avec exports CSV
- Repository Git local (scripts Python & SQL)

---

## 🐳 Lancer Kestra en local

```bash
docker compose up -d
```

Interface Web :
👉 http://localhost:8081

---

## ▶️ Vue d’ensemble du hands‑on

Vous allez orchestrer :

1. L’extraction Python :
   - `phase1_s3_to_staging.py`
2. Le chargement Staging → DWH
3. L’exécution des procédures :
   - `dw.sp_seed_dim_feature`
   - `dw.sp_load_staging_to_dw`
4. Le tout avec :
   - `batch_date = 2016-01-02`

---

## 🧩 Logique du flow Kestra

Le flow :
- Prend `batch_date` en paramètre
- Exécute les tâches séquentiellement
- Stoppe en cas d’erreur
- Centralise logs, retries et statuts

---

## 🐳 Exécution Python via Docker (recommandée)

La tâche Python utilise :

```yaml
containerImage: python:3.12-slim
```

Avantages :
- Isolation des dépendances
- Exécution reproductible
- Comportement proche de la production

⚠️ Nécessite :
```text
/var/run/docker.sock
```

---

## 🔁 Alternative : exécution Python locale (sans Docker socket)

Si vous ne souhaitez pas monter le socket Docker :

### Principe
- Supprimer `containerImage`
- Python installé sur le worker Kestra
- Dépendances installées manuellement

### Comparaison

| Docker | Local |
|------|------|
| Isolé | Plus simple |
| Reproductible | Dépend de la machine |
| Production‑like | Formation |

---

## 🔍 Observabilité

Kestra fournit :
- Historique d’exécution
- Logs par tâche
- Gestion des erreurs et retries

---

## ✅ Résultat attendu

Après une exécution réussie :
- Tables de staging alimentées
- Dimensions et faits chargés
- Données cohérentes pour le `batch_date`

---

## 🧠 À retenir

- Orchestration ≠ transformation
- Les paramètres permettent le backfill
- Kestra unifie Python, SQL et l’infrastructure
