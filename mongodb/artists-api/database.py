from functools import lru_cache

from pymongo import MongoClient
from pymongo.collection import Collection

from config import get_settings


@lru_cache(maxsize=1)
def get_mongo_client() -> MongoClient:
    """Réutilise une seule connexion PyMongo par processus."""
    settings = get_settings()
    return MongoClient(
        settings.mongo_uri,
        serverSelectionTimeoutMS=settings.mongo_timeout_ms,
    )


def get_artists_collection() -> Collection:
    # Dépendance FastAPI : remplaçable par une collection de test.
    settings = get_settings()
    return get_mongo_client()[settings.mongo_db][settings.mongo_collection]
