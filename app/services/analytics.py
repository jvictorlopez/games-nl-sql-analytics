from typing import Dict, Any, List, Optional, Tuple
import math
import pandas as pd
from functools import lru_cache
from rapidfuzz import process, fuzz
from .logger import get_logger, timeblock
from app.core.config import get_settings
from app.services.datastore import get_datastore

log = get_logger(__name__)
S = get_settings()

def _missing_frac(s: pd.Series) -> float:
    if len(s) == 0:
        return 0.0
    return float(s.isna().mean())

def _conf_weight(c: Optional[float], k: float, scale: float) -> float:
    if not S.ENABLE_CONFIDENCE_WEIGHT or c is None or math.isnan(c):
        return 1.0
    # simple sigmoid around k with scale
    return 1 / (1 + math.exp(-( (c - k) / scale )))

def _apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    out = df
    y, y0, y1 = filters.get("year"), filters.get("year_from"), filters.get("year_to")
    plat = filters.get("platform"); genre = filters.get("genre")
    publisher = filters.get("publisher"); dev = filters.get("developer")
    franchise = filters.get("franchise")

    if y is not None:
        out = out[out["Year_of_Release"] == int(y)]
    if y0 is not None:
        out = out[out["Year_of_Release"] >= int(y0)]
    if y1 is not None:
        out = out[out["Year_of_Release"] <= int(y1)]
    if plat:
        out = out[out["Platform"].astype(str).str.lower() == str(plat).lower()]
    if genre:
        out = out[out["Genre"].astype(str).str.lower() == str(genre).lower()]
    if publisher:
        out = out[out["Publisher"].astype(str).str.lower() == str(publisher).lower()]
    if dev:
        out = out[out["Developer"].astype(str).str.lower() == str(dev).lower()]
    if franchise:
        from app.utils.franchise import infer_franchise
        out = out[out["Name"].apply(lambda n: infer_franchise(n) == franchise)]
    return out

def _score_combo(row: pd.Series) -> Optional[float]:
    c = row.get("Critic_Score")
    u = row.get("User_Score")
    if pd.isna(c) and pd.isna(u):
        return None
    c_norm = 0.0 if pd.isna(c) else float(c)
    u_norm = 0.0 if pd.isna(u) else float(u)
    base = 0.6*c_norm + 0.4*u_norm
    # weights by counts
    cw = _conf_weight(row.get("Critic_Count"), k=20, scale=10)
    uw = _conf_weight(row.get("User_Count"), k=200, scale=100)
    return base * (0.5*cw + 0.5*uw)

@lru_cache(maxsize=128)
def dataset_summary() -> Dict[str, Any]:
    df = get_datastore().get_df()
    with timeblock(log, "summary"):
        years = df["Year_of_Release"].dropna().astype(int)
        return {
            "titles": int(len(df)),
            "years": {"min": int(years.min()) if not years.empty else None,
                      "max": int(years.max()) if not years.empty else None},
            "global_sales_sum": float(df["Global_Sales"].fillna(0).sum()) if "Global_Sales" in df else 0.0,
            "critic_score_avg": float(df["Critic_Score"].mean()) if "Critic_Score" in df else None,
            "user_score_avg": float(df["User_Score"].mean()) if "User_Score" in df else None,
            "missing": {
                "critic_score": _missing_frac(df["Critic_Score"]) if "Critic_Score" in df else 1.0,
                "user_score": _missing_frac(df["User_Score"]) if "User_Score" in df else 1.0,
                "critic_count": _missing_frac(df["Critic_Count"]) if "Critic_Count" in df else 1.0,
                "user_count": _missing_frac(df["User_Count"]) if "User_Count" in df else 1.0
            }
        }

def _prepare_items(df: pd.DataFrame, include_combo: bool) -> pd.DataFrame:
    cols = ["Name","Platform","Year_of_Release","Genre","Publisher",
            "Global_Sales","NA_Sales","EU_Sales","JP_Sales","Other_Sales",
            "Critic_Score","User_Score","Critic_Count","User_Count"]
    data = df[cols].copy()
    if include_combo:
        data["score_combo"] = df.apply(_score_combo, axis=1)
    return data

@lru_cache(maxsize=256)
def rankings(by: str = "global", n: int = 10, **filters) -> Dict[str, Any]:
    df = get_datastore().get_df()
    df_f = _apply_filters(df, filters)
    include_combo = by == "combo"
    data = _prepare_items(df_f, include_combo)
    key_map = {
        "global": "Global_Sales",
        "na": "NA_Sales",
        "eu": "EU_Sales",
        "jp": "JP_Sales",
        "other": "Other_Sales",
        "critic": "Critic_Score",
        "user": "User_Score",
        "combo": "score_combo"
    }
    col = key_map.get(by, "Global_Sales")
    data["_sort"] = data[col].fillna(-1e9)
    # tie-breakers
    data["_tie1"] = data["User_Count"].fillna(-1)
    data["_tie2"] = data["Critic_Count"].fillna(-1)
    data["_tie3"] = data["Global_Sales"].fillna(-1)
    data = data.sort_values(by=["_sort","_tie1","_tie2","_tie3","Name"],
                            ascending=[False,False,False,False,True]).head(int(n))
    items = []
    for _, r in data.iterrows():
        items.append({
            "name": r["Name"], "platform": r["Platform"], "year": int(r["Year_of_Release"]) if pd.notna(r["Year_of_Release"]) else None,
            "genre": r["Genre"], "publisher": r["Publisher"],
            "global_sales": r["Global_Sales"], "na_sales": r["NA_Sales"], "eu_sales": r["EU_Sales"],
            "jp_sales": r["JP_Sales"], "other_sales": r["Other_Sales"],
            "critic_score": r["Critic_Score"], "user_score": r["User_Score"],
            "critic_count": r["Critic_Count"], "user_count": r["User_Count"],
            "score_combo": (r["score_combo"] if include_combo else None)
        })
    return {"by": by, "filters": filters, "items": items}

def search(q: str, limit: int = 20) -> Dict[str, Any]:
    df = get_datastore().get_df()
    names = df["Name"].astype(str).tolist()
    hits = process.extract(q, names, scorer=fuzz.WRatio, limit=limit)
    # de-duplicate by name+platform+year best row
    results = []
    seen = set()
    for name, score, idx in hits:
        row = df.iloc[idx]
        key = (row["Name"], row.get("Platform"), row.get("Year_of_Release"))
        if key in seen:
            continue
        seen.add(key)
        results.append({
            "name": row["Name"],
            "platform": row.get("Platform"),
            "year": int(row["Year_of_Release"]) if pd.notna(row.get("Year_of_Release")) else None,
            "score": float(score)
        })
    return {"q": q, "hits": results}

def _percentile_within(series: pd.Series, value: Optional[float]) -> Optional[float]:
    s = series.dropna().astype(float)
    if s.empty or value is None or math.isnan(value):
        return None
    return float((s < value).mean()*100.0)

def game_details(name: str, platform: Optional[str]=None, year: Optional[int]=None) -> Dict[str, Any]:
    df = get_datastore().get_df()
    # exact match filter
    sub = df[df["Name"].astype(str).str.lower() == name.lower()]
    if platform:
        sub = sub[sub["Platform"].astype(str).str.lower() == platform.lower()]
    if year is not None:
        sub = sub[sub["Year_of_Release"] == int(year)]
    if sub.empty:
        # fuzzy fallback
        hits = search(name, limit=5)["hits"]
        return {"rows": [], "suggestions": hits, "percentiles_within_genre": {}, "neighbors": {"similar_by_genre_year": []}}

    # compute percentiles within genre of first row
    first = sub.iloc[0]
    genre = first.get("Genre")
    gdf = df[df["Genre"] == genre] if pd.notna(genre) else df
    details_rows = _prepare_items(sub, include_combo=True)

    p = {
        "critic": _percentile_within(gdf["Critic_Score"], first.get("Critic_Score")),
        "user": _percentile_within(gdf["User_Score"], first.get("User_Score")),
        "global_sales": _percentile_within(gdf["Global_Sales"], first.get("Global_Sales"))
    }

    # neighbors: same genre, year ±2
    y = first.get("Year_of_Release")
    neigh = gdf
    if pd.notna(y):
        neigh = gdf[(gdf["Year_of_Release"] >= int(y)-2) & (gdf["Year_of_Release"] <= int(y)+2)]
    neigh = _prepare_items(neigh, include_combo=True).sort_values(by=["score_combo","Global_Sales"], ascending=[False,False]).head(8)

    rows = []
    for _, r in details_rows.iterrows():
        rows.append({
            "name": r["Name"], "platform": r["Platform"], "year": int(r["Year_of_Release"]) if pd.notna(r["Year_of_Release"]) else None,
            "genre": r["Genre"], "publisher": r["Publisher"],
            "global_sales": r["Global_Sales"], "na_sales": r["NA_Sales"], "eu_sales": r["EU_Sales"],
            "jp_sales": r["JP_Sales"], "other_sales": r["Other_Sales"],
            "critic_score": r["Critic_Score"], "user_score": r["User_Score"],
            "critic_count": r["Critic_Count"], "user_count": r["User_Count"],
            "score_combo": r.get("score_combo")
        })
    neighbors = []
    for _, r in neigh.iterrows():
        neighbors.append({
            "name": r["Name"], "platform": r["Platform"], "year": int(r["Year_of_Release"]) if pd.notna(r["Year_of_Release"]) else None,
            "genre": r["Genre"], "publisher": r["Publisher"],
            "global_sales": r["Global_Sales"], "critic_score": r["Critic_Score"], "user_score": r["User_Score"],
            "score_combo": r.get("score_combo")
        })
    return {"rows": rows, "percentiles_within_genre": p, "neighbors": {"similar_by_genre_year": neighbors}}

