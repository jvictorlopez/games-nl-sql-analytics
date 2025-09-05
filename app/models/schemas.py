from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

class HealthOut(BaseModel):
    status: str
    version: str

class DatasetSummary(BaseModel):
    titles: int
    years: Dict[str, Optional[int]]  # {"min": ..., "max": ...}
    global_sales_sum: float
    critic_score_avg: Optional[float]
    user_score_avg: Optional[float]
    missing: Dict[str, float]  # fraction missing per column

class RankingItem(BaseModel):
    name: str
    platform: Optional[str] = None
    year: Optional[int] = None
    genre: Optional[str] = None
    publisher: Optional[str] = None
    global_sales: Optional[float] = None
    na_sales: Optional[float] = None
    eu_sales: Optional[float] = None
    jp_sales: Optional[float] = None
    other_sales: Optional[float] = None
    critic_score: Optional[float] = None
    user_score: Optional[float] = None
    score_combo: Optional[float] = None
    critic_count: Optional[float] = None
    user_count: Optional[float] = None

class RankingsOut(BaseModel):
    by: str
    filters: Dict[str, Any]
    items: List[RankingItem]

class SearchHit(BaseModel):
    name: str
    platform: Optional[str] = None
    year: Optional[int] = None
    score: float

class SearchOut(BaseModel):
    q: str
    hits: List[SearchHit]

class GameNeighbors(BaseModel):
    similar_by_genre_year: List[RankingItem]

class GameDetails(BaseModel):
    rows: List[RankingItem]
    percentiles_within_genre: Dict[str, Optional[float]]  # {"critic": p, "user": p, "global_sales": p}
    neighbors: GameNeighbors

class FranchiseStats(BaseModel):
    slug: str
    total_titles: int
    total_global_sales: float
    avg_critic: Optional[float]
    avg_user: Optional[float]
    top_entries: List[RankingItem]

