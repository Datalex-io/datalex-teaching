from pathlib import Path

import pytest
from pydantic import ValidationError

from config import Settings


def test_settings_use_environment_variables(monkeypatch):
    monkeypatch.setenv("MONGO_URI", "mongodb://mongo.example:27018")
    monkeypatch.setenv("MONGO_DB", "other_db")
    monkeypatch.setenv("MONGO_COLLECTION", "people")
    monkeypatch.setenv("MONGO_TIMEOUT_MS", "5000")
    monkeypatch.setenv("APP_HOST", "0.0.0.0")
    monkeypatch.setenv("APP_PORT", "9000")
    monkeypatch.setenv("ARTISTS_DATA_FILE", "data/alternative.json")

    settings = Settings(_env_file=None)
    assert settings.mongo_uri == "mongodb://mongo.example:27018"
    assert settings.mongo_db == "other_db"
    assert settings.mongo_collection == "people"
    assert settings.mongo_timeout_ms == 5000
    assert settings.app_host == "0.0.0.0"
    assert settings.app_port == 9000
    assert settings.data_path.name == "alternative.json"
    assert settings.data_path.is_absolute()


def test_invalid_port_rejected(monkeypatch):
    monkeypatch.setenv("MONGO_URI", "mongodb://127.0.0.1:27017")
    monkeypatch.setenv("MONGO_DB", "cinema")
    monkeypatch.setenv("MONGO_COLLECTION", "artists")
    monkeypatch.setenv("APP_PORT", "70000")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_collection_name_comes_from_settings(monkeypatch):
    from types import SimpleNamespace

    import database

    target_collection = object()

    class FakeDatabase:
        def __getitem__(self, name):
            assert name == "artists_from_env"
            return target_collection

    class FakeClient:
        def __getitem__(self, name):
            assert name == "db_from_env"
            return FakeDatabase()

    settings = SimpleNamespace(mongo_db="db_from_env", mongo_collection="artists_from_env")
    monkeypatch.setattr(database, "get_settings", lambda: settings)
    monkeypatch.setattr(database, "get_mongo_client", lambda: FakeClient())
    assert database.get_artists_collection() is target_collection
