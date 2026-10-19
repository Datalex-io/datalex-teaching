import re

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from config import get_settings
from database import get_artists_collection
from models import Artist, ArtistUpdate

app = FastAPI(title="Artists API", version="1.1.0")


@app.exception_handler(PyMongoError)
def mongo_error_handler(request, exc):
    return JSONResponse(status_code=503, content={"detail": "MongoDB unavailable"})


@app.get("/artists", response_model=list[Artist], response_model_exclude_unset=True)
def list_artists(
    name: str | None = None,
    artist_id: str | None = Query(default=None, alias="_id"),
    collection: Collection = Depends(get_artists_collection),
):
    """Liste tous les artistes, avec filtres optionnels cumulables."""
    query = {}
    if artist_id is not None:
        query["_id"] = artist_id

    if name and name.strip():
        # re.escape évite d'interpréter les caractères saisis comme du regex.
        pattern = re.escape(name.strip())
        query["$or"] = [
            {"first_name": {"$regex": pattern, "$options": "i"}},
            {"last_name": {"$regex": pattern, "$options": "i"}},
        ]

    return list(collection.find(query))


@app.get("/artists/{artist_id}", response_model=Artist, response_model_exclude_unset=True)
def get_artist(artist_id: str, collection: Collection = Depends(get_artists_collection)):
    artist = collection.find_one({"_id": artist_id})
    if artist is None:
        raise HTTPException(status_code=404, detail="Artist not found")
    return artist


@app.patch("/artists/{artist_id}", response_model=Artist, response_model_exclude_unset=True)
@app.put("/artists/{artist_id}", response_model=Artist, response_model_exclude_unset=True)
def update_artist(
    artist_id: str,
    changes: ArtistUpdate,
    collection: Collection = Depends(get_artists_collection),
):
    # $set ne modifie que les clés envoyées dans le JSON.
    fields = changes.model_dump(exclude_unset=True)
    result = collection.update_one({"_id": artist_id}, {"$set": fields})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Artist not found")
    return collection.find_one({"_id": artist_id})


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run("main:app", host=settings.app_host, port=settings.app_port, reload=True)
