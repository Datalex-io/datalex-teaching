# Artists API

API REST FastAPI + MongoDB local. Données : `data/artists.json` (205 artistes).

## Installation

Python 3.10+ et MongoDB démarré en local.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows : .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # Windows : copy .env.example .env
```

Vérifier les paramètres MongoDB dans `.env`.

## Démarrage

```bash
python seed.py
python main.py
```

API : http://127.0.0.1:8000 — Swagger : http://127.0.0.1:8000/docs

Le port dépend de `APP_PORT` dans `.env`.

## Routes

| Méthode | URL | Résultat |
| --- | --- | --- |
| GET | `/artists` | Tous les artistes |
| GET | `/artists?name=Coppola` | Filtre sur prénom ou nom |
| GET | `/artists?_id=artist:1` | Filtre par `_id` |
| GET | `/artists/artist:1` | Un artiste |
| PATCH / PUT | `/artists/artist:1` | Modification partielle par JSON |

```bash
curl -X PATCH http://127.0.0.1:8000/artists/artist:1 \
  -H "Content-Type: application/json" \
  -d '{"first_name":"Sofia","hobbies":["photo"]}'
```

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

Architecture et requêtes MongoDB : [DEVELOPMENT.MD](DEVELOPMENT.MD).
