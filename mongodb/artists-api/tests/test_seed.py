import mongomock

from config import PROJECT_DIR
from seed import import_artists


def test_import_only_artists():
    db = mongomock.MongoClient()["cinema"]
    total, inserted = import_artists(db["artists"], PROJECT_DIR / "data/artists.json")
    assert (total, inserted) == (205, 205)
    assert db.list_collection_names() == ["artists"]


def test_import_preserves_existing_data():
    collection = mongomock.MongoClient()["cinema"]["artists"]
    source = PROJECT_DIR / "data/artists.json"
    import_artists(collection, source)
    collection.update_one({"_id": "artist:1"}, {"$set": {"first_name": "Updated"}})
    total, inserted = import_artists(collection, source)
    assert (total, inserted) == (205, 0)
    assert collection.count_documents({}) == 205
    assert collection.find_one({"_id": "artist:1"})["first_name"] == "Updated"
