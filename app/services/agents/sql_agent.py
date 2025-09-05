# api/app/services/agents/sql_agent.py
from typing import Dict, Any, Tuple, List
import duckdb
import os
from app.services.nlu import Intent

# Path to CSV; set in .env or docker compose
CSV_PATH = os.environ.get("GAMES_CSV", "/app/data/base_jogos.csv")

_con = None

def _ensure_view(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create or replace a typed view over the CSV on THIS connection.
    IMPORTANT: Do NOT use prepared parameters inside CREATE VIEW.
    """
    # Escape single quotes to safely inline as SQL string literal
    path_lit = CSV_PATH.replace("'", "''")

    con.execute(f"""
        CREATE OR REPLACE VIEW main.games AS
        SELECT
          Name,
          Platform,
          TRY_CAST(
            NULLIF(
              CASE
                WHEN LOWER(TRIM(Year_of_Release)) IN ('', 'n/a', 'na', 'tbd', '-') THEN NULL
                ELSE TRIM(Year_of_Release)
              END,
              ''
            ) AS INT
          ) AS Year_of_Release,
          Genre,
          Publisher,
          TRY_CAST(NULLIF(TRIM(NA_Sales),    '') AS DOUBLE) AS NA_Sales,
          TRY_CAST(NULLIF(TRIM(EU_Sales),    '') AS DOUBLE) AS EU_Sales,
          TRY_CAST(NULLIF(TRIM(JP_Sales),    '') AS DOUBLE) AS JP_Sales,
          TRY_CAST(NULLIF(TRIM(Other_Sales), '') AS DOUBLE) AS Other_Sales,
          TRY_CAST(NULLIF(TRIM(Global_Sales),'') AS DOUBLE) AS Global_Sales,
          TRY_CAST(NULLIF(LOWER(TRIM(Critic_Score)),'tbd') AS DOUBLE) AS Critic_Score,
          TRY_CAST(NULLIF(TRIM(Critic_Count), '') AS INT) AS Critic_Count,
          TRY_CAST(NULLIF(LOWER(TRIM(User_Score)),'tbd') AS DOUBLE) AS User_Score,
          TRY_CAST(NULLIF(TRIM(User_Count),  '' ) AS INT) AS User_Count,
          Developer,
          Rating
        FROM read_csv_auto('{path_lit}', header = true, all_varchar = true);
    """)

def _conn() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        _con = duckdb.connect()  # in-memory, single-process
        _ensure_view(_con)       # build the view immediately
        # Warm-up to ensure the view exists before any NL→SQL runs
        _con.execute("SELECT 1 FROM main.games LIMIT 1;")
    return _con

def _build_top_sql(intent: Intent) -> str:
    metric = intent.metric or "Global_Sales"
    limit = max(1, min(intent.top_n or 10, 100))
    where = ["1=1"]
    if intent.year:
        where.append(f"Year_of_Release = {intent.year}")
    if intent.year_from and intent.year_to:
        where.append(f"Year_of_Release BETWEEN {intent.year_from} AND {intent.year_to}")
    if intent.name_like:
        like = intent.name_like.replace("'", "''")
        where.append(f"LOWER(Name) LIKE LOWER('%{like}%')")

    sql = f"""
    SELECT
      Name,
      Platform,
      Year_of_Release AS year,
      Genre,
      Publisher,
      {metric} AS {metric}
    FROM games
    WHERE {' AND '.join(where)}
    ORDER BY {metric} DESC NULLS LAST
    LIMIT {limit}
    """
    return sql.strip()

def _build_aggregate_sql(intent: Intent) -> str:
    field = intent.agg_field or "User_Score"
    where = ["1=1"]
    if intent.year:
        where.append(f"Year_of_Release = {intent.year}")
    if intent.year_from and intent.year_to:
        where.append(f"Year_of_Release BETWEEN {intent.year_from} AND {intent.year_to}")
    if intent.name_like:
        like = intent.name_like.replace("'", "''")
        where.append(f"LOWER(Name) LIKE LOWER('%{like}%')")

    sql = f"""
    SELECT
      AVG({field}) AS avg_{field}
    FROM games
    WHERE {' AND '.join(where)}
    """
    return sql.strip()

def build_sql(intent: Intent) -> Tuple[str, Dict[str, Any]]:
    if intent.task == "aggregate":
        sql = _build_aggregate_sql(intent)
        suggestion = {"type": None}
        return sql, suggestion
    # default top
    sql = _build_top_sql(intent)
    suggestion = {
        "type": "bar",
        "title": "Ranking por vendas",
        "x": "Name",
        "y": intent.metric or "Global_Sales",
    }
    return sql, suggestion

def run_sql(sql: str) -> Tuple[List[str], List[List]]:
    con = _conn()
    cur = con.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return cols, rows
