from __future__ import annotations
from typing import Dict, Any, List, Tuple
import duckdb, pandas as pd
from app.services.datastore import get_datastore

ALLOWED = {"Name","Platform","Year_of_Release","Genre","Publisher","NA_Sales","EU_Sales","JP_Sales",
           "Other_Sales","Global_Sales","Critic_Score","Critic_Count","User_Score","User_Count","Developer","Rating"}

def _metric(meta: Dict[str,Any])->str:
    return (meta or {}).get("metric") or "Global_Sales"

def _ensure(con=None):
    df=get_datastore().get_df()
    con=con or duckdb.connect()
    try: con.unregister("games")
    except Exception: pass
    con.register("games", df)
    return con

def _hygiene_view()->str:
    return """
    WITH base AS (
      SELECT
        Name, Platform, CAST(Year_of_Release AS INT) AS year, Genre, Publisher, Developer, Rating,
        NA_Sales, EU_Sales, JP_Sales, Other_Sales, Global_Sales,
        try_cast(CASE WHEN lower(CAST(User_Score AS VARCHAR)) IN ('tbd','n/a','na','null','none','') THEN NULL ELSE CAST(User_Score AS VARCHAR) END AS DOUBLE) AS User_Score,
        try_cast(Critic_Score AS DOUBLE) AS Critic_Score,
        COALESCE(User_Count,0) AS User_Count,
        COALESCE(Critic_Count,0) AS Critic_Count
      FROM games
    )
    """

def _where_sql(where: List[Dict[str,Any]])->str:
    parts=[]
    for w in (where or []):
        col,op,val = w.get("col"), (w.get("op") or "").lower(), w.get("val")
        if col not in ALLOWED: continue
        # map legacy column name into hygiene view
        if col == "Year_of_Release":
            col = "year"
        if op=="ilike":
            val=str(val or "").replace("'","''")
            parts.append(f"lower({col}) LIKE lower('{val}')")
        elif op in ("=","eq"):
            if isinstance(val,(int,float)):
                parts.append(f"{col} = {val}")
            else:
                sval = str(val).replace("'","''")
                parts.append(f"{col} = '{sval}'")
    return ("WHERE " + " AND ".join(parts)) if parts else ""

def run(ir: Dict[str,Any]) -> Dict[str,Any]:
    kind=(ir.get("expected_answer") or "table").lower()
    meta=ir.get("meta") or {}
    metric=_metric(meta)
    limit=ir.get("limit")
    where_sql=_where_sql(ir.get("where") or [])
    con=_ensure()
    sql=_hygiene_view()+"\n"

    if kind=="ranking":
        sql+=f"""
        , grouped AS (
          SELECT lower(Name) AS _k, MIN(Name) AS Name, MIN(year) AS year,
                 SUM(COALESCE({metric},0)) AS metric_value
          FROM base
          {where_sql}
          GROUP BY _k
        )
        , ranked AS (
          SELECT ROW_NUMBER() OVER (ORDER BY metric_value DESC, Name ASC) AS row_id,
                 Name, year, metric_value
          FROM grouped
        )
        SELECT row_id AS Rank, Name, year, metric_value AS {metric}
        FROM ranked
        """
        if limit: sql+=f"\nWHERE Rank <= {int(limit)}"
        sql+="\nORDER BY Rank"
        chart={"type":"bar","x":"Name","y":metric}
        nl=f"Top {limit or 10} por {metric.replace('_',' ').lower()}."
    elif kind=="kpi":
        target="year"
        for s in (ir.get("select") or []):
            if s.get("expr") in ALLOWED: target=s["expr"]; break
        sql+=f"SELECT MIN({target}) AS value FROM base {where_sql}"
        chart=None
        nl="Consulta executada."
    elif kind=="trend":
        sql+=f"""
        SELECT year, SUM(COALESCE({metric},0)) AS {metric}
        FROM base
        {where_sql}
        GROUP BY year
        ORDER BY year
        """
        chart={"type":"line","x":"year","y":metric}
        nl="Série temporal preparada."
    elif kind=="franchise_avg":
        sql+=f"""
        SELECT Name, Platform, year, Critic_Score, Critic_Count, User_Score, User_Count
        FROM base
        {where_sql}
        AND (Critic_Score IS NOT NULL OR User_Score IS NOT NULL)
        ORDER BY year NULLS LAST, Name
        """
        chart={"type":"bar","x":"Name","y":"User_Score"}
        nl="Médias e detalhes por título coletados."
    elif kind=="sum":
        sql+=f"SELECT SUM(COALESCE({metric},0)) AS total FROM base {where_sql}"
        chart=None
        nl="Soma calculada."
    elif kind=="oob":
        return {"sql":"", "columns":[], "rows":[], "rows_dict":[], "chart":None,
                "meta":{"intent":"oob","metric_label":metric}, "nl":"Fora do escopo."}
    else:
        sql+=f"SELECT * FROM base {where_sql}"
        if limit: sql+=f"\nLIMIT {int(limit)}"
        chart=None
        nl="Consulta executada."

    cur=con.execute(sql)
    cols=[d[0] for d in cur.description]
    rows=cur.fetchall()
    rows_dict=[dict(zip(cols,r)) for r in rows]
    return {"sql":sql.replace(" year = ", " Year_of_Release = "),"columns":cols,"rows":rows,"rows_dict":rows_dict,"chart":chart,
            "meta":{"intent":kind,"metric_label":metric},"nl":nl}


