from __future__ import annotations
from typing import Dict, Any, List
import duckdb
import pandas as pd

from app.services.datastore import get_datastore


def ensure_table_registered(con: duckdb.DuckDBPyConnection | None = None) -> duckdb.DuckDBPyConnection:
    df = get_datastore().get_df()
    con = con or duckdb.connect()
    try:
        con.unregister("games")
    except Exception:
        pass
    con.register("games", df)
    return con


def run(ir: Dict[str, Any]) -> Dict[str, Any]:
    if ir.get("expected_answer") == "oos":
        return {"sql": "", "columns": [], "rows": [], "rows_dict": [], "chart": None, "meta": {"metric":"Global_Sales"}}

    con = ensure_table_registered()

    # Build SQL
    if ir.get("expected_answer") == "ranking":
        metric = (ir.get("meta") or {}).get("metric") or "Global_Sales"
        topn = (ir.get("meta") or {}).get("topn") or ir.get("limit") or 10
        year = (ir.get("meta") or {}).get("year")
        where = f"WHERE Year_of_Release = {int(year)}" if year else ""
        sql = f"""
        WITH base AS (
          SELECT Name, Year_of_Release, {metric} AS metric_value
          FROM games
          {where}
        ),
        grouped AS (
          SELECT lower(Name) AS _k, MIN(Name) AS Name,
                 MIN(CAST(Year_of_Release AS INT)) AS year,
                 SUM(COALESCE(metric_value,0)) AS metric_value
          FROM base
          GROUP BY _k
        ),
        ranked AS (
          SELECT Name, year, metric_value,
                 ROW_NUMBER() OVER (ORDER BY metric_value DESC, Name ASC) AS Rank
          FROM grouped
        )
        SELECT Rank, Name, year, metric_value AS {metric}
        FROM ranked
        WHERE Rank <= {int(topn)}
        ORDER BY Rank
        """
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        rows_dict = [dict(zip(cols, r)) for r in rows]
        chart = {"type": "bar", "x": "Name", "y": metric}
        meta = {"metric_label": metric, "metric": metric, **(ir.get("meta") or {})}
        return {"sql": sql, "columns": cols, "rows": rows, "rows_dict": rows_dict, "chart": chart, "meta": meta}
    select_parts: List[str] = []
    for s in ir.get("select", []):
        expr = s.get("expr")
        alias = s.get("alias")
        select_parts.append(f"{expr} AS {alias}" if alias else expr)
    for a in ir.get("aggregates", []):
        fn = a.get("fn")
        col = a.get("col")
        alias = a.get("alias")
        if fn == "avg_user":
            # Some CSVs encode 'tbd' or other non-numeric strings. Use CASE for broader safety.
            select_parts.append(
                f"ROUND(AVG(CASE WHEN lower(CAST(User_Score AS VARCHAR)) IN ('tbd','n/a','na','null','none','') THEN NULL ELSE try_cast(User_Score AS DOUBLE) END),2) AS {alias}"
            )
        else:
            select_parts.append(f"{fn.upper()}({col}) AS {alias}")
    if not select_parts:
        select_parts = ["Name", "CAST(Year_of_Release AS INT) AS year"]

    where_parts: List[str] = []
    for w in ir.get("where", []):
        if "expr" in w:
            where_parts.append(w["expr"])
        else:
            col = w.get("col")
            op = w.get("op")
            val = w.get("val")
            if op == "ilike":
                where_parts.append(f"lower({col}) LIKE lower('{val}')")
            elif op == "eq":
                where_parts.append(f"{col} = '{val}'")

    sql = "SELECT " + ", ".join(select_parts) + " FROM games"
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)
    if ir.get("group_by"):
        sql += " GROUP BY " + ", ".join(ir["group_by"])
    order = ir.get("order_by") or []
    if order:
        sql += " ORDER BY " + ", ".join([f"{o['expr']} {o.get('dir','desc').upper()}" for o in order])
    if ir.get("limit"):
        sql += f" LIMIT {int(ir['limit'])}"

    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    rows_dict = [dict(zip(cols, r)) for r in rows]

    metric = (ir.get("meta") or {}).get("metric") or (cols[-1] if cols else "Global_Sales")
    chart = None
    hint = ir.get("chart_hint")
    if hint == "bar":
        chart = {"type": "bar", "x": "Name", "y": metric}
    elif hint == "line":
        chart = {"type": "line", "x": "year", "y": metric}
    else:
        chart = {"type": "table", "x": "Name", "y": metric}

    meta = {"metric_label": metric, "metric": metric}
    meta.update(ir.get("meta") or {})
    return {"sql": sql, "columns": cols, "rows": rows, "rows_dict": rows_dict, "chart": chart, "meta": meta}


