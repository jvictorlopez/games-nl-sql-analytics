from fastapi import APIRouter, Query
from typing import Optional
from app.models.schemas import RankingsOut, SearchOut, GameDetails
from app.services.analytics import rankings, search, game_details

router = APIRouter(prefix="/games", tags=["games"])

@router.get("/rankings", response_model=RankingsOut)
def get_rankings(
    by: str = Query("global", pattern="^(global|na|eu|jp|other|critic|user|combo)$"),
    n: int = 10,
    year: Optional[int] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    platform: Optional[str] = None,
    genre: Optional[str] = None,
    publisher: Optional[str] = None,
    developer: Optional[str] = None,
    franchise: Optional[str] = None
):
    filters = dict(year=year, year_from=year_from, year_to=year_to, platform=platform,
                   genre=genre, publisher=publisher, developer=developer, franchise=franchise)
    return rankings(by=by, n=n, **filters)

@router.get("/search", response_model=SearchOut)
def search_games(q: str, limit: int = 20):
    return search(q, limit=limit)

@router.get("/{name}", response_model=GameDetails)
def details(name: str, platform: Optional[str] = None, year: Optional[int] = None):
    return game_details(name, platform=platform, year=year)

