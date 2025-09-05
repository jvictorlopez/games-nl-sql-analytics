from __future__ import annotations
import re
from typing import Any, Dict, Optional, Tuple, List

import duckdb
import pandas as pd

from app.core.config import get_settings
from app.services.datastore import get_datastore
from app.services.analytics import search as fuzzy_search, rankings as compute_rankings, dataset_summary
from app.utils.nl_map import BY_MAP, FIELD_MAP, ACTION_HINTS

S = get_settings()

ALLOWED_BY = {"global":"Global_Sales","na":"NA_Sales","eu":"EU_Sales","jp":"JP_Sales","other":"Other_Sales",
              "critic":"Critic_Score","user":"User_Score","combo":"score_combo"}

SAFE_COLUMNS = {
    "Platform":"Platform", "Genre":"Genre", "Publisher":"Publisher", "Developer":"Developer",
    "Year_of_Release": "Year_of_Release", "Name":"Name"
}

def ensure_table_registered(con: duckdb.DuckDBPyConnection | None = None) -> duckdb.DuckDBPyConnection:
    df = get_datastore().get_df()
    con = con or duckdb.connect()
    con.register("games", df)
    return con

def _find_by_token(q: str) -> Optional[str]:
    ql = q.lower()
    for key, toks in BY_MAP.items():
        if any(t in ql for t in toks):
            return key
    # heuristic
    if "venda" in ql or "sales" in ql:
        return "global"
    if "nota" in ql or "score" in ql:
        return "critic"
    return None

def _find_action(q: str) -> str:
    ql = q.lower()
    for act, toks in ACTION_HINTS.items():
        if any(t in ql for t in toks):
            return act
    # default: rankings
    return "rankings"

def _extract_ints(q: str) -> List[int]:
    return [int(x) for x in re.findall(r"\b(19\d{2}|20\d{2}|21\d{2})\b", q)]

def _extract_topn(q: str) -> Optional[int]:
    m = re.search(r"\btop\s*(\d{1,3})\b", q.lower())
    if m:
        return int(m.group(1))
    m2 = re.search(r"\b(\d{1,3})\s*(melhores|best)\b", q.lower())
    if m2:
        return int(m2.group(1))
    return None

def _find_filter(q: str, field_key: str) -> Optional[str]:
    ql = q.lower()
    for word in FIELD_MAP[field_key]:
        m = re.search(fr"{word}\s*[:=]\s*([A-Za-z0-9\-\s\+\/\.\&]+)", ql)
        if m:
            return m.group(1).strip()
    return None

def rule_based_plan(q: str) -> Dict[str, Any]:
    action = _find_action(q)
    by = _find_by_token(q) or ("global" if action == "rankings" else None)
    years = _extract_ints(q)
    year = years[0] if years else None
    topn = _extract_topn(q) or (10 if action == "rankings" else None)

    filters = {}
    for fk in ["platform","genre","publisher","developer","franchise"]:
        v = _find_filter(q, fk)
        if v: filters[fk] = v

    if year:
        filters["year"] = year

    plan = {"action": action, "by": by, "n": topn, "filters": filters}
    return plan

# ---------- LLM Assist (optional) ----------
def llm_plan(q: str, api_key: Optional[str]) -> Optional[Dict[str, Any]]:
    if not api_key:
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        sys = (
            "You convert a natural-language question about a videogame CSV into JSON parameters ONLY.\n"
            "Output exactly this JSON schema:\n"
            '{"action":"rankings|summary|details|franchise","by":"global|na|eu|jp|other|critic|user|combo|null",'
            '"n":int|null,"filters":{"year":int|null,"year_from":int|null,"year_to":int|null,'
            '"platform":str|null,"genre":str|null,"publisher":str|null,"developer":str|null,"franchise":str|null,'
            '"name":str|null}}\n'
            "Never include any prose, just the JSON. If you are unsure, put nulls."
        )
        rsp = client.chat.completions.create(
            model="gpt-4o-mini",  # compact & reliable; interchangeable with newer models
            messages=[{"role":"system","content":sys},{"role":"user","content":q}],
            response_format={"type":"json_object"},
            temperature=0.0,
        )
        j = rsp.choices[0].message
        import json
        data = json.loads(j.content)
        return data
    except Exception:
        return None

# ---------- Presence / coverage ----------
def dataset_covers(plan: Dict[str, Any], q: str) -> Tuple[bool, Optional[str]]:
    # If a specific 'name' is requested, check fuzzy presence
    name = (plan.get("filters") or {}).get("name")
    if not name:
        # Try to detect name in quotes: "Game Name"
        m = re.search(r'"([^"]+)"', q)
        if m:
            name = m.group(1)
            plan.setdefault("filters", {})["name"] = name
    if name:
        hits = fuzzy_search(name, limit=1)["hits"]
        if not hits:
            return False, f'Jogo "{name}" não encontrado no dataset.'
        # OK, dataset has a likely match
    # In general, the dataset has breadth across years/platforms/genres → assume covered
    return True, None

# ---------- SQL generation & execution ----------
def _filters_to_where(filters: Dict[str, Any]) -> str:
    parts = []
    if not filters: return ""
    if filters.get("year") is not None:
        parts.append(f'Year_of_Release = {int(filters["year"])}')
    if filters.get("year_from") is not None:
        parts.append(f'Year_of_Release >= {int(filters["year_from"])}')
    if filters.get("year_to") is not None:
        parts.append(f'Year_of_Release <= {int(filters["year_to"])}')
    for col in ["Platform","Genre","Publisher","Developer"]:
        k = col.lower()
        v = filters.get(k)
        if v:
            # equality on normalized case
            parts.append(f"lower({col}) = lower('{str(v).replace("'","''")}')")
    if filters.get("franchise"):
        # simple LIKE heuristic
        v = filters["franchise"]
        parts.append(f"lower(Name) LIKE lower('%{str(v).replace("'","''")}%')")
    if filters.get("name"):
        v = filters["name"]
        parts.append(f"lower(Name) = lower('{str(v).replace("'","''")}')")
    return (" WHERE " + " AND ".join(parts)) if parts else ""

def generate_sql(plan: Dict[str, Any]) -> Tuple[str, str]:
    action = plan.get("action","rankings")
    filters = plan.get("filters") or {}
    where = _filters_to_where(filters)

    if action == "summary":
        sql = (
            "SELECT COUNT(*) AS titles, MIN(Year_of_Release) AS min_year, "
            "MAX(Year_of_Release) AS max_year, SUM(COALESCE(Global_Sales,0)) AS global_sales_sum, "
            "AVG(Critic_Score) AS critic_score_avg, AVG(User_Score) AS user_score_avg FROM games" + where
        )
        return sql, "summary"

    if action == "details" and filters.get("name"):
        sql = (
            "SELECT Name, Platform, Year_of_Release, Genre, Publisher, Global_Sales, "
            "NA_Sales, EU_Sales, JP_Sales, Other_Sales, Critic_Score, User_Score, "
            "Critic_Count, User_Count FROM games" + where + " LIMIT 20"
        )
        return sql, "details"

    # default rankings
    by_key = plan.get("by") or "global"
    sort_col = ALLOWED_BY.get(by_key, "Global_Sales")
    n = int(plan.get("n") or 10)
    # add combo score column when needed: same logic as analytics._score_combo simplified for SQL
    select = (
        "SELECT Name, Platform, Year_of_Release, Genre, Publisher, "
        "Global_Sales, NA_Sales, EU_Sales, JP_Sales, Other_Sales, "
        "Critic_Score, User_Score, Critic_Count, User_Count"
    )
    if by_key == "combo":
        # simple combined score proxy (no sigmoid here for simplicity)
        select += ", (0.6*COALESCE(Critic_Score,0) + 0.4*COALESCE(User_Score,0)) AS score_combo"
    sql = f"{select} FROM games{where} ORDER BY COALESCE({sort_col}, -1e9) DESC, COALESCE(User_Count,-1) DESC, COALESCE(Critic_Count,-1) DESC, COALESCE(Global_Sales,-1) DESC, Name ASC LIMIT {n}"
    return sql, "rankings"

def execute_sql(sql: str) -> pd.DataFrame:
    con = ensure_table_registered()
    return con.sql(sql).df()


