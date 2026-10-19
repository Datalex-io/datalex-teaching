from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    """Configuration issue du .env ou des variables du système."""

    mongo_uri: str
    mongo_db: str
    mongo_collection: str
    mongo_timeout_ms: int = Field(default=3000, gt=0)

    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    artists_data_file: Path = Path("data/artists.json")

    model_config = SettingsConfigDict(
        env_file=PROJECT_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def data_path(self) -> Path:
        # Un chemin relatif pointe toujours vers le projet, pas vers le terminal.
        return PROJECT_DIR / self.artists_data_file


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
