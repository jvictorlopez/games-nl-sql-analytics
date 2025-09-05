from typing import Optional, Dict, Any
import re
import duckdb
import pandas as pd
from app.core.config import get_settings

S = get_settings()

REGION_MAP = {
    "global": "Global_Sales",
    "mundial": "Global_Sales",
    "mundo": "Global_Sales",
    "na": "NA_Sales", "norte america": "NA_Sales", "eua": "NA_Sales",
    "eu": "EU_Sales", "europa": "EU_Sales",
    "jp": "JP_Sales", "japao": "JP_Sales", "japão": "JP_Sales",
    "other": "Other_Sales", "resto": "Other_Sales",
}

def _extract_years(q: str) -> Dict[str, int]:
    ql = q.lower()
    m = re.search(r"\b(19[89]\d|20[0-2]\d)\b", ql)
    year = int(m.group(1)) if m else None
    m2 = re.search(r"(de|from)\s+(19[89]\d|20[0-2]\d)\s+(a|até|to|-)\s+(19[89]\d|20[0-2]\d)", ql)
    yrs: Dict[str, int] = {}
    if m2:
        yrs["year_from"] = int(m2.group(2))
        yrs["year_to"] = int(m2.group(4))
    elif year:
        yrs["year_exact"] = year
    return yrs

def _extract_topn(q: str, default: int = 10) -> int:
    m = re.search(r"\btop\s*(\d{1,3})\b", q.lower())
    if m:
        return int(m.group(1))
    if "top" in q.lower():
        return default
    return default

def _extract_region_metric(q: str) -> str:
    ql = q.lower()
    if any(k in ql for k in ["critic", "crítica", "critica"]):
        return "Critic_Score"
    if any(k in ql for k in ["user", "usuário", "usuarios", "usuários"]):
        return "User_Score"
    for k, col in REGION_MAP.items():
        if k in ql:
            return col
    return "Global_Sales"

def _extract_filter(q: str, label: str):
    if label == "platform":
        m = re.search(r"\b(NES|SNES|GB|DS|3DS|Wii|WiiU|PS2|PS3|PS4|PS5|X360|XB|XOne|Switch)\b", q, re.I)
        return m.group(0) if m else None
    if label == "genre":
        m = re.search(r"\b(Action|Racing|Sports|Platform|Shooter|Role-?Playing|Puzzle|Misc|Adventure|Simulation|Fighting|Strategy)\b", q, re.I)
        return m.group(0) if m else None
    return None

def nl_to_sql_plan(q: str, df: pd.DataFrame) -> Dict[str, Any]:
    ql = q.lower()
    topn = _extract_topn(ql)
    years = _extract_years(ql)
    metric = _extract_region_metric(ql)
    platform = _extract_filter(q, "platform")
    genre = _extract_filter(q, "genre")

    if any(k in ql for k in ["por plataforma", "by platform"]):
        group = "Platform"
    elif any(k in ql for k in ["por gênero", "por genero", "by genre"]):
        group = "Genre"
    elif any(k in ql for k in ["publisher", "publicadora", "editora"]):
        group = "Publisher"
    else:
        group = "Name"

    return {
        "metric": metric,
        "group": group,
        "topn": topn,
        **years,
        "platform": platform,
        "genre": genre,
    }

def plan_to_sql(plan: Dict[str, Any]) -> Dict[str, Any]:
    metric = plan["metric"]
    group = plan["group"]
    topn = plan["topn"]
    where = ["1=1"]
    if plan.get("year_exact"): where.append(f"Year_of_Release = {plan['year_exact']}")
    if plan.get("year_from"): where.append(f"Year_of_Release >= {plan['year_from']}")
    if plan.get("year_to"):   where.append(f"Year_of_Release <= {plan['year_to']}")
    if plan.get("platform"):  where.append(f"Platform = '{plan['platform']}'")
    if plan.get("genre"):     where.append(f"Genre = '{plan['genre']}'")

    if group == "Name":
        sql = f"""
        SELECT Name, Platform, Year_of_Release AS year, Genre, Publisher, {metric}
        FROM games
        WHERE {' AND '.join(where)}
        ORDER BY {metric} DESC
        LIMIT {topn}
        """
        plot = {"kind": "bar", "x": "Name", "y": metric, "title": "Top por " + metric}
    else:
        agg_col = "AVG" if "Score" in metric else "SUM"
        alias = "value"
        sql = f"""
        SELECT {group}, {agg_col}({metric}) AS {alias}
        FROM games
        WHERE {' AND '.join(where)}
        GROUP BY {group}
        ORDER BY {alias} DESC
        LIMIT {topn}
        """
        plot = {"kind": "bar", "x": group, "y": alias, "title": f"Top {group} por {metric}"}

    if group == "Name" and metric in ("Critic_Score", "User_Score"):
        sql = f"""
        SELECT Name, Critic_Score, User_Score, COALESCE(User_Count, 1) AS User_Count
        FROM games
        WHERE {' AND '.join(where)}
        ORDER BY User_Score DESC
        LIMIT {topn}
        """
        plot = {"kind": "scatter", "x": "Critic_Score", "y": "User_Score", "size": "User_Count",
                "title": "Crítica vs Usuários"}

    return {"sql": " ".join(sql.split()), "plot_hint": plot}

def generate_sql(q: str, df: pd.DataFrame) -> str:
    plan = nl_to_sql_plan(q, df)
    return plan_to_sql(plan)["sql"]

def run_duckdb_sql(sql: str, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect()
    con.register(table_name, df)
    try:
        out = con.execute(sql).fetchdf()
    finally:
        con.close()
    return out

def sql_with_plot(q: str, df: pd.DataFrame):
    plan = nl_to_sql_plan(q, df)
    out = plan_to_sql(plan)
    return out["sql"], out["plot_hint"]


