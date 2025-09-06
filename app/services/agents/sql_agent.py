from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
import json
import re
import duckdb
import pandas as pd
import time
import logging

from app.core.llm import chat
from app.services.datastore import get_datastore

# ======== SYSTEM PROMPT (SQL AGENT) ========
SQL_AGENT_SYS_PROMPT = """
You are the SQL Agent for a Games Analytics service.
Your job: given a PLAN JSON, produce ONE DuckDB SQL and a minimal projection for the UI.

DATASET/TABLE: "games" with columns:
['Name','Platform','Year_of_Release','Genre','Publisher','NA_Sales','EU_Sales',
 'JP_Sales','Other_Sales','Global_Sales','Critic_Score','Critic_Count',
 'User_Score','User_Count','Developer','Rating']

STRICT OUTPUT: ONLY a JSON (no prose):
{
  "projection": [...],
  "sql": "SELECT ...",            // read-only, safe casts on scores
  "chart": {"type":"bar","x":"Name","y":"<metric>"},
  "meta": {"metric_label":"<metric>"}
}

METRICS:
- global->Global_Sales, na->NA_Sales, eu->EU_Sales, jp->JP_Sales, other->Other_Sales
- critic->try_cast(Critic_Score AS DOUBLE)
- user->try_cast(User_Score AS DOUBLE)
- combo->(0.6*try_cast(Critic_Score AS DOUBLE)+0.4*try_cast(User_Score AS DOUBLE))

RANKINGS (intent=rankings) HARD RULES:
- Build base(Name, Year_of_Release, metric_value) using the chosen metric and optional filters (e.g., year).
- **Aggregate by title across platforms**:
    GROUP BY lower(Name)
    SELECT MIN(Name) AS Name, MIN(Year_of_Release) AS year, SUM(COALESCE(metric_value,0)) AS metric_value
- Rank with ROW_NUMBER() OVER (ORDER BY metric_value DESC, Name ASC) starting at 1.
- Projection must be ["Rank","Name","year","<metric>"].
- Limit to topN (PLAN.topn or 10).

FRANCHISE AVERAGE (intent=franchise_avg):
- WHERE lower(Name) LIKE lower('%<franchise>%').
- Return detail table:
  ["Name","Platform","year","Critic_Score","Critic_Count","User_Score","User_Count"]
- chart.y = "User_Score"; meta.metric_label="User_Score".

GENERAL TABLE HYGIENE:
- For score-focused intents (franchise_avg, details when returning scores), only include rows where at least one score exists:
  (try_cast(Critic_Score AS DOUBLE) IS NOT NULL OR try_cast(User_Score AS DOUBLE) IS NOT NULL)
- Rankings by sales/metrics keep rows as usual (COALESCE already applied).

SUMMARY (intent=summary):
- One row with: titles count, min year, max year, sum Global_Sales, avg(Critic_Score), avg(User_Score).
- Use try_cast on scores. Projection: suitable summary columns.

DETAILS (intent=details):
- One title by name (or LIKE), limit 20 rows, include useful columns.

SAFETY:
- Only SELECT/CTEs from "games". Never write/DDL.
"""

ALLOWED_COLS = {
    "Name","Platform","Year_of_Release","Genre","Publisher","NA_Sales","EU_Sales","JP_Sales",
    "Other_Sales","Global_Sales","Critic_Score","Critic_Count","User_Score","User_Count","Developer","Rating"
}


def ensure_table_registered(con: duckdb.DuckDBPyConnection | None = None) -> duckdb.DuckDBPyConnection:
    df = get_datastore().get_df()
    con = con or duckdb.connect()
    try:
        existing = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    except Exception:
        existing = []
    if "games" in existing:
        try:
            con.unregister("games")
        except Exception:
            pass
    con.register("games", df)
    return con


def run_franchise_total_sales(plan: Dict[str, Any]) -> Dict[str, Any]:
    franchise = ((plan.get("filters") or {}).get("franchise") or "").replace("'", "''")
    sql = f"""
    SELECT
      SUM(COALESCE(Global_Sales,0)) AS Global_Sales,
      SUM(COALESCE(NA_Sales,0))     AS NA_Sales,
      SUM(COALESCE(EU_Sales,0))     AS EU_Sales,
      SUM(COALESCE(JP_Sales,0))     AS JP_Sales,
      SUM(COALESCE(Other_Sales,0))  AS Other_Sales,
      COUNT(*) AS Titles
    FROM games
    WHERE lower(Name) LIKE lower('%{franchise}%')
    """
    con = ensure_table_registered()
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    rows_dict = [dict(zip(cols, r)) for r in rows]
    chart = {
        "type": "bar",
        "x": "region",
        "y": "sales",
        "data": [
            {"region": "Global", "sales": rows[0][0] if rows else 0},
            {"region": "NA",     "sales": rows[0][1] if rows else 0},
            {"region": "EU",     "sales": rows[0][2] if rows else 0},
            {"region": "JP",     "sales": rows[0][3] if rows else 0},
            {"region": "Other",  "sales": rows[0][4] if rows else 0},
        ],
    }
    meta = {"metric_label": "Global_Sales"}
    return {"sql": sql, "columns": cols, "rows": rows, "rows_dict": rows_dict, "chart": chart, "meta": meta}


def _validate_sql(sql: str, intent: str) -> None:
    s = sql.strip().lower()
    if not (s.startswith("with") or s.startswith("select")):
        raise ValueError("SQL must be a read-only SELECT/CTE.")
    forbidden = ["insert","update","delete","drop","alter","create","attach","copy","pragma","call","vacuum"]
    if any(tok in s for tok in forbidden):
        raise ValueError("Only read-only statements allowed.")
    if intent == "rankings":
        # must aggregate, not dedup via partition
        if "group by" not in s:
            raise ValueError("Rankings devem agregar por título (GROUP BY).")
        if re.search(r"partition\s+by\s+lower\s*\(\s*name\s*\)", s):
            raise ValueError("Do not use PARTITION BY for dedup; aggregate with GROUP BY.")


def _rows_to_dicts(cols: List[str], rows: List[tuple]) -> List[Dict[str, Any]]:
    return [dict(zip(cols, r)) for r in rows]


def _metric_label_from_plan(plan: Dict[str, Any]) -> str:
    m = (plan.get("metric") or "global").lower()
    return {
        "global":"Global_Sales","na":"NA_Sales","eu":"EU_Sales","jp":"JP_Sales","other":"Other_Sales",
        "critic":"Critic_Score","user":"User_Score","combo":"score_combo"
    }.get(m, "Global_Sales")


def llm_build_sql_and_run(question: str, plan: Dict[str, Any]) -> Dict[str, Any]:
    trace: List[Dict[str, Any]] = []
    metric_lbl_initial = _metric_label_from_plan(plan)
    trace.append({
        "step": "intent_detected",
        "intent": plan.get("intent"),
        "metric": metric_lbl_initial,
        "filters": plan.get("filters") or {},
        "topn": plan.get("topn")
    })

    # 1) Ask LLM for exact SQL
    msg = [{"role":"system","content":SQL_AGENT_SYS_PROMPT},
           {"role":"user","content":json.dumps({"plan":plan}, ensure_ascii=False)}]
    j: Optional[Dict[str, Any]] = None
    try:
        out = chat(msg, temperature=0.0)
        if out:
            try:
                j = json.loads(out)
            except Exception:
                j = None
    except Exception as e:
        logging.getLogger("nl2sql").warning("SQL-Agent LLM unavailable, using fallback. %s", e)
        j = None

    # 2) Fallbacks per intent if LLM fails
    if not j:
        j = _fallback_json(plan)

    sql = j.get("sql", "")
    projection = j.get("projection") or []
    chart = j.get("chart") or {"type":"bar","x":"Name","y":_metric_label_from_plan(plan)}
    meta = j.get("meta") or {"metric_label": _metric_label_from_plan(plan)}

    trace.append({"step":"sql_built","projection":projection, "by": meta.get("metric_label")})

    # 3) Guard/validate and execute with soft fallback
    try:
        _validate_sql(sql, plan.get("intent","rankings"))
    except Exception as ex:
        trace.append({"step":"exception","phase":"validate","message":str(ex)[:200]})
        j = _fallback_json(plan)
        sql = j["sql"]
        projection = j.get("projection") or []
        chart = j.get("chart") or {"type":"bar","x":"Name","y":_metric_label_from_plan(plan)}
        meta = j.get("meta") or {"metric_label": _metric_label_from_plan(plan)}
        trace.append({"step":"sql_built","projection":projection, "by": meta.get("metric_label"), "mode":"fallback"})

    con = ensure_table_registered()
    t0 = time.time()
    try:
        cur = con.execute(sql)
    except Exception as ex:
        trace.append({"step":"exception","phase":"execute","message":str(ex)[:200]})
        j = _fallback_json(plan)
        sql = j["sql"]
        projection = j.get("projection") or []
        chart = j.get("chart") or {"type":"bar","x":"Name","y":_metric_label_from_plan(plan)}
        meta = j.get("meta") or {"metric_label": _metric_label_from_plan(plan)}
        cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    trace.append({"step":"sql_executed","rowcount": len(rows), "time_ms": int((time.time()-t0)*1000)})

    # 4) Reorder to projection if provided
    if projection:
        df = pd.DataFrame(rows, columns=cols)
        keep = [c for c in projection if c in df.columns]
        if keep:
            df = df[keep]
            cols, rows = list(df.columns), [tuple(x) for x in df.itertuples(index=False, name=None)]

    rows_dict = _rows_to_dicts(cols, rows)

    # 5) If franchise_avg, compute weighted meta from the returned detail rows; add extra charts and hist
    if plan.get("intent") == "franchise_avg":
        try:
            dff = pd.DataFrame(rows_dict)
            for c in ["Critic_Score","User_Score","Critic_Count","User_Count"]:
                if c in dff.columns:
                    dff[c] = pd.to_numeric(dff[c], errors="coerce")
            cc = float(dff.get("Critic_Count", pd.Series(dtype=float)).fillna(0).sum())
            uc = float(dff.get("User_Count", pd.Series(dtype=float)).fillna(0).sum())
            cwa = float((dff.get("Critic_Score", pd.Series(dtype=float))*dff.get("Critic_Count", pd.Series(dtype=float))).fillna(0).sum()/cc) if cc>0 else None
            uwa = float((dff.get("User_Score", pd.Series(dtype=float))*dff.get("User_Count", pd.Series(dtype=float))).fillna(0).sum()/uc) if uc>0 else None
            combo = None
            if (cwa is not None) or (uwa is not None):
                combo = (0.6*(cwa or 0.0) + 0.4*(uwa or 0.0))

            meta.update({
                "franchise_weighted": {
                    "critic_wavg": cwa, "user_wavg": uwa, "combo_wavg": combo,
                    "critic_count_sum": int(cc), "user_count_sum": int(uc),
                    "total_titles": int(len(dff))
                }
            })

            charts_extra: List[Dict[str, Any]] = []
            if (uwa is not None) or (cwa is not None) or (combo is not None):
                charts_extra.append({
                    "type": "bar_kv",
                    "title": "Médias ponderadas (Usuários, Críticos, Geral 60/40)",
                    "data": [
                        ["Usuários (wavg)", uwa if uwa is not None else None],
                        ["Críticos (wavg)", cwa if cwa is not None else None],
                        ["Geral 60/40",    combo if combo is not None else None],
                    ],
                })
            if "User_Score" in dff.columns and dff["User_Score"].notna().any():
                charts_extra.append({"type":"hist", "title":"Distribuição de User_Score", "column":"User_Score"})
            if "Critic_Score" in dff.columns and dff["Critic_Score"].notna().any():
                charts_extra.append({"type":"hist", "title":"Distribuição de Critic_Score", "column":"Critic_Score"})
            if charts_extra:
                meta["extra_charts"] = charts_extra

            # histograms
            try:
                import numpy as np  # optional
                def _hist(series: pd.Series) -> Dict[str, Any]:
                    s = series.dropna()
                    if s.empty:
                        return {"bins": [], "counts": []}
                    mn, mx = float(s.min()), float(s.max())
                    if mn == mx:
                        bins = [mn, mx]
                        counts = [int(len(s))]
                    else:
                        bins = list(np.linspace(mn, mx, 11))
                        cuts = pd.cut(s, bins=bins, include_lowest=True)
                        counts = cuts.value_counts(sort=False).tolist()
                    return {"bins": bins, "counts": counts}
            except Exception:
                def _hist(series: pd.Series) -> Dict[str, Any]:
                    s = series.dropna()
                    if s.empty:
                        return {"bins": [], "counts": []}
                    mn, mx = float(s.min()), float(s.max())
                    step = (mx - mn) / 10 if mx > mn else 1.0
                    bins = [mn + i*step for i in range(11)]
                    cuts = pd.cut(s, bins=bins, include_lowest=True)
                    counts = cuts.value_counts(sort=False).tolist()
                    return {"bins": bins, "counts": counts}

            meta["hist"] = {
                "user": _hist(dff.get("User_Score", pd.Series(dtype=float))),
                "critic": _hist(dff.get("Critic_Score", pd.Series(dtype=float)))
            }
        except Exception:
            pass

    trace.append({"step":"postprocess","actions":["projection_order","rows_dict","meta_weights","hist" if meta.get("hist") else ""]})

    meta["intent"] = plan.get("intent")
    return {"sql": sql, "columns": cols, "rows": rows, "rows_dict": rows_dict, "chart": chart, "meta": meta, "trace": trace}


# --- Reliable fallbacks (short & correct) ---

def _fallback_json(plan: Dict[str, Any]) -> Dict[str, Any]:
    intent = plan.get("intent","rankings")
    if intent == "franchise_avg":
        fam = (plan.get("filters") or {}).get("franchise") or ""
        fam = fam.replace("'", "''")
        sql = f"""
        SELECT
          Name,
          Platform,
          CAST(Year_of_Release AS INT) AS year,
          try_cast(Critic_Score AS DOUBLE) AS Critic_Score,
          COALESCE(Critic_Count,0) AS Critic_Count,
          try_cast(User_Score AS DOUBLE) AS User_Score,
          COALESCE(User_Count,0) AS User_Count
        FROM games
        WHERE lower(Name) LIKE lower('%{fam}%')
          AND (
            try_cast(Critic_Score AS DOUBLE) IS NOT NULL
            OR try_cast(User_Score AS DOUBLE) IS NOT NULL
          )
        ORDER BY year NULLS LAST, Name ASC
        """
        return {
          "projection":["Name","Platform","year","Critic_Score","Critic_Count","User_Score","User_Count"],
          "sql": sql,
          "chart":{"type":"bar","x":"Name","y":"User_Score"},
          "meta":{"metric_label":"User_Score"}
        }

    if intent == "summary":
        sql = """
        SELECT
          COUNT(*) AS titles,
          MIN(Year_of_Release) AS min_year,
          MAX(Year_of_Release) AS max_year,
          SUM(COALESCE(Global_Sales,0)) AS global_sales_sum,
          AVG(try_cast(Critic_Score AS DOUBLE)) AS critic_score_avg,
          AVG(try_cast(User_Score AS DOUBLE))   AS user_score_avg
        FROM games
        """
        return {"projection":["titles","min_year","max_year","global_sales_sum","critic_score_avg","user_score_avg"],
                "sql": sql, "chart":{"type":"bar","x":"min_year","y":"global_sales_sum"},
                "meta":{"metric_label":"Global_Sales"}}

    if intent == "details":
        name = ((plan.get("filters") or {}).get("name") or "").replace("'","''")
        sql = f"""
        SELECT Name, Platform, Year_of_Release AS year, Genre, Publisher, Developer, Rating,
               Global_Sales, NA_Sales, EU_Sales, JP_Sales, Other_Sales,
               try_cast(Critic_Score AS DOUBLE) AS Critic_Score,
               try_cast(User_Score AS DOUBLE)   AS User_Score
        FROM games
        WHERE lower(Name) LIKE lower('%{name}%')
        LIMIT 20
        """
        return {"projection":["Name","Platform","year","Genre","Publisher","Developer","Rating","Global_Sales","NA_Sales","EU_Sales","JP_Sales","Other_Sales","Critic_Score","User_Score"],
                "sql": sql, "chart":{"type":"bar","x":"Platform","y":"Global_Sales"},
                "meta":{"metric_label":"Global_Sales"}}

    # rankings fallback (aggregate by title)
    metric_label = _metric_label_from_plan(plan)
    metric_expr = {
        "Global_Sales":"Global_Sales","NA_Sales":"NA_Sales","EU_Sales":"EU_Sales","JP_Sales":"JP_Sales","Other_Sales":"Other_Sales",
        "Critic_Score":"try_cast(Critic_Score AS DOUBLE)","User_Score":"try_cast(User_Score AS DOUBLE)",
        "score_combo":"(0.6*try_cast(Critic_Score AS DOUBLE)+0.4*try_cast(User_Score AS DOUBLE))"
    }[metric_label]
    year = (plan.get("filters") or {}).get("year")
    topn = int(plan.get("topn") or 10)
    where = f"WHERE Year_of_Release = {int(year)}" if year else ""
    sql = f"""
    WITH base AS (
      SELECT Name, Year_of_Release, {metric_expr} AS metric_value
      FROM games
      {where}
    ),
    grouped AS (
      SELECT
        lower(Name) AS _k,
        MIN(Name) AS Name,
        MIN(Year_of_Release) AS year,
        SUM(COALESCE(metric_value,0)) AS metric_value
      FROM base
      GROUP BY _k
    ),
    ranked AS (
      SELECT Name, year, metric_value,
             ROW_NUMBER() OVER (ORDER BY metric_value DESC, Name ASC) AS Rank
      FROM grouped
    )
    SELECT Rank, Name, year, metric_value AS {metric_label}
    FROM ranked
    WHERE Rank <= {topn}
    ORDER BY Rank
    """
    return {"projection":["Rank","Name","year",metric_label],
            "sql": sql, "chart":{"type":"bar","x":"Name","y":metric_label},
            "meta":{"metric_label":metric_label}}

