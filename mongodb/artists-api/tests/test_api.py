import mongomock
import pytest
from fastapi.testclient import TestClient
from pymongo.errors import ServerSelectionTimeoutError

from config import PROJECT_DIR
from database import get_artists_collection
from main import app
from seed import import_artists


@pytest.fixture
def client():
    collection = mongomock.MongoClient()["test_cinema"]["test_artists"]
    import_artists(collection, PROJECT_DIR / "data/artists.json")

    # On remplace la dépendance PyMongo ; aucun serveur local n'est nécessaire.
    app.dependency_overrides[get_artists_collection] = lambda: collection
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_list_all(client):
    response = client.get("/artists")
    assert response.status_code == 200
    assert len(response.json()) == 205
    assert response.json()[0]["_id"] == "artist:1"
    assert "hobbies" not in response.json()[0]


def test_filter_by_name_case_insensitive(client):
    response = client.get("/artists", params={"name": "coPPola"})
    assert response.status_code == 200
    assert {artist["_id"] for artist in response.json()} == {"artist:1", "artist:181"}


def test_filter_by_first_name(client):
    response = client.get("/artists", params={"name": "Sofia"})
    assert [a["_id"] for a in response.json()] == ["artist:1"]


def test_filter_by_id(client):
    response = client.get("/artists", params={"_id": "artist:1"})
    assert response.status_code == 200
    assert response.json()[0]["first_name"] == "Sofia"
    assert len(response.json()) == 1


def test_filter_by_missing_id(client):
    assert client.get("/artists", params={"_id": "artist:9999"}).json() == []


def test_get_one(client):
    response = client.get("/artists/artist:3")
    assert response.status_code == 200
    assert response.json()["last_name"] == "Hitchcock"


def test_get_missing(client):
    assert client.get("/artists/artist:9999").status_code == 404


@pytest.mark.parametrize("method", ["put", "patch"])
def test_update(client, method):
    response = getattr(client, method)(
        "/artists/artist:1", json={"birth_date": None, "hobbies": ["cinema"]}
    )
    assert response.status_code == 200
    assert response.json()["birth_date"] is None
    assert response.json()["hobbies"] == ["cinema"]
    assert response.json()["last_name"] == "Coppola"
    assert client.get("/artists/artist:1").json()["hobbies"] == ["cinema"]


def test_update_missing(client):
    assert client.patch("/artists/artist:9999", json={"first_name": "X"}).status_code == 404


@pytest.mark.parametrize("body", [{}, {"_id": "artist:5"}, {"last_name": None}, {"other": 1}])
def test_invalid_update(client, body):
    assert client.patch("/artists/artist:1", json=body).status_code == 422


def test_name_regex_is_literal(client):
    assert client.get("/artists", params={"name": ".*"}).json() == []


def test_filter_name_and_id(client):
    r = client.get("/artists", params={"name": "Sofia", "_id": "artist:1"})
    assert len(r.json()) == 1
    r = client.get("/artists", params={"name": "Alfred", "_id": "artist:1"})
    assert r.json() == []


def test_mongodb_unavailable_returns_503():
    class OfflineCollection:
        def find(self, query):
            raise ServerSelectionTimeoutError("server offline")

    app.dependency_overrides[get_artists_collection] = lambda: OfflineCollection()
    try:
        with TestClient(app) as test_client:
            response = test_client.get("/artists")
        assert response.status_code == 503
        assert response.json() == {"detail": "MongoDB unavailable"}
    finally:
        app.dependency_overrides.clear()
