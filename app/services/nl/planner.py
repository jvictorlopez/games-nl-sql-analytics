from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
import re

from app.services.nl.entity_normalizer import resolve_title, _region as _region_from, _year as _year_from


def build_ir(question: str, signals: Dict[str, Any] | None = None) -> Dict[str, Any]:
    q = (question or "").strip()
    ql = q.lower()
    year = _year_from(q)
    metric = _metric(ql)
    topn = _topn(ql) or 10

    # Detect intents
    if any(w in ql for w in ["banana","preço","preco","clima","restaurante","uber","bitcoin","dólar","dolar","imposto","vacina"]):
        return {"select": [], "from": "games", "where": [], "group_by": [], "order_by": [], "limit": None,
                "aggregates": [], "expected_answer": "oos", "chart_hint": "table", "meta": {"metric": metric, "topn": None, "year": year}}

    # Franchise averages
    if ("média" in ql or "media" in ql or "average" in ql or "nota" in ql or "rating" in ql) and ("franquia" in ql or "franchise" in ql or "zelda" in ql or "mario" in ql):
        fran = None
        for cand in ["zelda","mario","pokemon","final fantasy","call of duty"]:
            if cand in ql:
                fran = cand
                break
        where = []
        if fran:
            where.append({"col":"Name","op":"ilike","val":f"%{fran}%"})
        return {
            "select": [],
            "from": "games",
            "where": where,
            "group_by": [],
            "order_by": [],
            "limit": None,
            "aggregates": [
                {"fn":"avg","col":"Critic_Score","alias":"avg_critic_100"},
                {"fn":"avg_user","col":"User_Score","alias":"avg_user_10"}
            ],
            "expected_answer": "franchise_avg",
            "chart_hint": "table",
            "meta": {"metric": "User_Score", "topn": None, "year": year, "franchise": fran}
        }

    # Details / year-of-release for a title
    if any(w in ql for w in ["qual o ano","quando saiu","quando lançou","what year","release year"]):
        res = resolve_title(q)
        if not res.get("canonical"):
            return {"select": [], "from": "games", "where": [], "group_by": [], "order_by": [], "limit": None, "aggregates": [], "expected_answer": "not_found", "chart_hint": "table", "meta": {"metric": "Year_of_Release", "topn": 0, "entity_resolutions": res}}
        where = [{"col":"Name","op":"ilike","val":f"%{res['canonical']}%"}]
        return {
            "select": [{"expr":"MIN(CAST(Year_of_Release AS INT))","alias":"year"}],
            "from": "games",
            "where": where,
            "group_by": [],
            "order_by": [],
            "limit": 1,
            "aggregates": [],
            "expected_answer": "kpi",
            "chart_hint": "table",
            "meta": {"metric": "Year_of_Release", "topn": 1, "year": year, "entity_resolutions": res}
        }

    # Ranking default
    where = []
    if year is not None:
        where.append({"expr":"CAST(Year_of_Release AS INT) = %d" % year})
    order = [{"expr": metric, "dir": "desc"}]
    return {
        "select": [
            {"expr":"Name","alias":"Name"},
            {"expr":"CAST(Year_of_Release AS INT)","alias":"year"},
            {"expr": metric, "alias": metric}
        ],
        "from": "games",
        "where": where,
        "group_by": [],
        "order_by": order,
        "limit": topn,
        "aggregates": [],
        "expected_answer": "ranking",
        "chart_hint": "bar",
        "meta": {"metric": metric, "topn": topn, "year": year}
    }


def _metric(ql: str) -> str:
    if any(w in ql for w in ["jap", "japão", "japan", "jp"]):
        return "JP_Sales"
    if any(w in ql for w in ["europ", "europe", "eu"]):
        return "EU_Sales"
    if any(w in ql for w in ["américa do norte", "america do norte", "na", "eua", "us", "usa"]):
        return "NA_Sales"
    if any(w in ql for w in ["critic", "crítico", "critico", "crítica", "critica", "metacritic"]):
        return "Critic_Score"
    if any(w in ql for w in ["user", "usuário", "usuario", "userscore", "nota de usuário"]):
        return "User_Score"
    return "Global_Sales"


def _topn(ql: str) -> Optional[int]:
    m = re.search(r"\btop\s*(\d{1,3})\b", ql)
    return int(m.group(1)) if m else None


