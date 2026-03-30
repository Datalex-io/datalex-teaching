# 🎬 DVD Rental API (FastAPI + Neon PostgreSQL)

Cette application est une petite API construite avec **FastAPI** permettant d'interroger la base de données classique `dvdrental`, hébergée sur **Neon** (PostgreSQL Serverless). 

Pour des raisons de sécurité, les identifiants de connexion sont stockés dans un fichier d'environnement (`.env`).

## 📋 Prérequis

Avant de lancer le projet, assurez-vous d'avoir :
* **Python 3.8** ou supérieur installé sur votre machine.
* Un compte [Neon](https://neon.tech/) avec une base de données contenant le dataset `dvdrental` restauré (via `pg_restore`).
* Votre chaîne de connexion Neon (Connection String).

## 🚀 Installation

1.  **Clonez ou créez le projet :**
    Placez le fichier `main.py` contenant le code de l'API dans un dossier dédié.

2.  **Installez les dépendances :**
    Ouvrez votre terminal dans le dossier du projet et exécutez la commande suivante. (Notez l'ajout de `python-dotenv` pour gérer les variables d'environnement).
    ```bash
    pip install fastapi uvicorn psycopg2-binary python-dotenv
    ```

## ⚙️ Configuration (Variables d'environnement)

1. **Créez un fichier `.env` :**
   À la racine de votre projet (au même niveau que `main.py`), créez un fichier nommé exactement `.env`.

2. **Ajoutez votre chaîne de connexion :**
   Ouvrez ce fichier `.env` et ajoutez-y votre lien Neon (sans espaces autour du `=`) :
   ```env
   DATABASE_URL="postgresql://[user]:[password]@[ep-name].eu-central-1.aws.neon.tech/dvdrental?sslmode=require"
   ```
   *(Attention : Si vous utilisez Git, n'oubliez pas d'ajouter le fichier `.env` à votre `.gitignore` pour ne pas le publier !)*

3. **Utilisation dans le code (`main.py`) :**
   Votre fichier `main.py` doit maintenant utiliser `dotenv` pour charger cette variable de manière sécurisée :
   ```python
   from fastapi import FastAPI, HTTPException
   import psycopg2
   from psycopg2.extras import RealDictCursor
   import os
   from dotenv import load_dotenv

   # Charge les variables du fichier .env
   load_dotenv()

   app = FastAPI(title="DVD Rental API")

   # Récupère la chaîne de connexion de manière sécurisée
   DATABASE_URL = os.getenv("DATABASE_URL")

   # ... suite de votre code (get_db_connection, routes, etc.)
   ```

## 💻 Démarrage du serveur

Pour lancer l'API en mode développement (avec rechargement automatique à chaque modification du code), tapez cette commande dans votre terminal :

```bash
uvicorn main:app --reload
```

Le serveur démarrera localement sur le port **8000**.

## 📡 Endpoints de l'API

L'API expose les routes suivantes :

* **`GET /getfilms`**
    * **Description :** Retourne une liste de films.
    * **Paramètre optionnel :** `limit` (entier, par défaut à 50) pour limiter le nombre de résultats.
    * **Exemple :** `http://127.0.0.1:8000/getfilms?limit=10`

* **`GET /getcustomer/{customer_id}`**
    * **Description :** Retourne l'historique détaillé des locations (titre du film, date de location, date de retour) pour un client spécifique.
    * **Exemple :** `http://127.0.0.1:8000/getcustomer/1` (Affiche les locations de Mary Smith).

## 📖 Documentation Interactive (Swagger UI)

FastAPI génère automatiquement une documentation interactive pour tester vos routes directement depuis le navigateur. 
Une fois le serveur lancé, rendez-vous sur : 👉 **http://127.0.0.1:8000/docs**

---

## 🌍 Optionnel : Exposer l'API sur Internet avec Ngrok

Si vous souhaitez tester votre API depuis un autre appareil (comme votre téléphone) ou la partager temporairement avec un collaborateur sans avoir à la déployer sur un vrai serveur, vous pouvez utiliser **Ngrok**.

1. **Installez Ngrok :**
   Téléchargez-le depuis [ngrok.com](https://ngrok.com/download) et suivez les instructions pour l'authentifier avec votre token gratuit.

2. **Lancez votre API localement :**
   Assurez-vous que votre serveur Uvicorn tourne bien sur le port 8000 (`uvicorn main:app --reload`).

3. **Exposez le port avec Ngrok :**
   Ouvrez un *nouveau* terminal (laissez l'autre tourner) et tapez la commande suivante :
   ```bash
   ngrok http 8000
   ```

4. **Récupérez votre URL publique :**
   Ngrok va générer une URL HTTPS temporaire (ex: `https://a1b2-c3d4.ngrok-free.app`). 
   Vous pouvez maintenant accéder à votre API et à la documentation Swagger de n'importe où via `https://a1b2-c3d4.ngrok-free.app/docs`.

*(Note : Dès que vous fermez le terminal Ngrok, le lien public expire).*