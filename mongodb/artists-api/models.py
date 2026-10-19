from pydantic import BaseModel, ConfigDict, Field, model_validator


class Artist(BaseModel):
    # Dans ce jeu de données, les _id sont des chaînes, pas des ObjectId.
    id: str = Field(alias="_id")
    first_name: str
    last_name: str
    birth_date: str | None = None
    hobbies: list[str] | None = None


class ArtistUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    first_name: str | None = None
    last_name: str | None = None
    birth_date: str | None = None
    hobbies: list[str] | None = None

    @model_validator(mode="after")
    def check_update(self):
        # Les champs absents restent inchangés ; _id n'est jamais modifiable.
        if not self.model_fields_set:
            raise ValueError("Provide at least one field")
        for name in ("first_name", "last_name"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} cannot be null")
        return self
