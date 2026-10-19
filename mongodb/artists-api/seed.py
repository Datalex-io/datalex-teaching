import json
from pathlib import Path

from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from config import get_settings
from database import get_artists_collection, get_mongo_client


def import_artists(collection: Collection, source: Path) -> tuple[int, int]:
    """Import initial : ajoute les absents sans écraser les artistes existants."""
    artists = json.loads(source.read_text(encoding="utf-8"))
    inserted = 0

    for artist in artists:
        # L'égalité sur _id est réutilisée lors de l'upsert.
        # $setOnInsert évite de perdre les modifications déjà faites via l'API.
        fields = {key: value for key, value in artist.items() if key != "_id"}
        result = collection.update_one(
            {"_id": artist["_id"]},
            {"$setOnInsert": fields},
            upsert=True,
        )
        if result.upserted_id is not None:
            inserted += 1

    return len(artists), inserted


def main() -> None:
    settings = get_settings()
    try:
        get_mongo_client().admin.command("ping")
        total, inserted = import_artists(get_artists_collection(), settings.data_path)
    except PyMongoError as exc:
        raise SystemExit(f"MongoDB inaccessible: {exc}") from exc
    print(f"{settings.mongo_db}.{settings.mongo_collection}: {total} lignes, {inserted} insérées")


if __name__ == "__main__":
    main()
