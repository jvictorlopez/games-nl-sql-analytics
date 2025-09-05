# api/app/services/agents/orchestrator.py
from typing import Dict, Any
from app.services.nlu import parse_query, is_gaming_domain, Intent


def _sql_for_top_by_year(region_col: str, year: int, top_n: int) -> str:
    """Generate SQL with TRY_CAST for robust year filtering."""
    return f"""
        SELECT
          Name,
          Platform,
          Year_of_Release AS year,
          Genre,
          Publisher,
          {region_col} AS metric
        FROM games
        WHERE TRY_CAST(Year_of_Release AS INT) = {int(year)}
        ORDER BY {region_col} DESC NULLS LAST
        LIMIT {int(top_n)}
    """


def _sql_for_aggregate(agg: str, metric: str, intent: Intent) -> str:
    """Aggregate over a metric with safe casts and optional filters."""
    agg_upper = (agg or "AVG").upper()
    metric_col = metric or "User_Score"
    # cast-safe numeric expression
    expr = f"TRY_CAST({metric_col} AS DOUBLE)"

    where = ["1=1"]
    if intent.year is not None:
        where.append(f"TRY_CAST(Year_of_Release AS INT) = {int(intent.year)}")
    if intent.year_from is not None and intent.year_to is not None:
        where.append(
            f"TRY_CAST(Year_of_Release AS INT) BETWEEN {int(intent.year_from)} AND {int(intent.year_to)}"
        )
    if intent.name_like:
        like = intent.name_like.replace("'", "''")
        where.append(f"LOWER(Name) LIKE LOWER('%{like}%')")

    return (
        f"SELECT {agg_upper}({expr}) AS {agg_upper.lower()}_{metric_col} "
        f"FROM games WHERE {' AND '.join(where)}"
    )


def route_query(q: str) -> Dict[str, Any]:
    """
    Orchestrates which agent to use.
    If it's a gaming-domain question and we can compute from local CSV, route to SQL.
    If out of domain, bounce. (Web search remains off by default.)
    """
    if not is_gaming_domain(q):
        return {
            "route": "bounce",
            "reason": "Pergunta fora do domínio de games. Tente algo como vendas, plataformas, notas, anos, etc.",
        }

    intent: Intent = parse_query(q)

    # Aggregate path
    if intent.task == "aggregate":
        metric = intent.agg_field or "User_Score"
        sql = _sql_for_aggregate(intent.agg or "AVG", metric, intent)
        from app.services.agents.sql_agent import run_sql

        cols, rows = run_sql(sql)
        return {
            "route": "sql",
            "intent": intent,
            "reason": "Média de user score conforme filtros." if (intent.agg or "AVG").lower()=="avg" else "Agregado conforme filtros.",
            "sql": sql,
            "columns": cols,
            "rows": rows,
            "chart": None,
        }

    # SQL path for rankings/top
    if intent.task == "top" and intent.year is not None:
        region_col = intent.metric or "Global_Sales"
        sql = _sql_for_top_by_year(region_col, intent.year, intent.top_n or 10)
        from app.services.agents.sql_agent import run_sql

        cols, rows = run_sql(sql)
        return {
            "route": "sql",
            "intent": intent,
            "reason": _reason_for(intent),
            "sql": sql,
            "columns": cols,
            "rows": rows,
            "chart": {"type": "bar", "x": "Name", "y": "metric"},
        }

    # Default: try SQL fallback (top global)
    sql = (
        """
        SELECT Name, Platform, TRY_CAST(Year_of_Release AS INT) AS year, Genre, Publisher, Global_Sales AS metric
        FROM games
        WHERE 1=1
        ORDER BY Global_Sales DESC NULLS LAST
        LIMIT 10
        """
    )
    from app.services.agents.sql_agent import run_sql

    cols, rows = run_sql(sql)
    return {
        "route": "sql",
        "intent": intent,
        "reason": _reason_for(intent),
        "sql": sql,
        "columns": cols,
        "rows": rows,
        "chart": {"type": "bar", "x": "Name", "y": "metric"},
    }


def _reason_for(intent: Intent) -> str:
    if intent.task == "top":
        base = (
            "Top por vendas"
            if intent.metric == "Global_Sales"
            else f"Top por {intent.metric.replace('_', ' ').lower()}"
        )
        if intent.year:
            return f"{base} no ano {intent.year}."
        if intent.year_from and intent.year_to:
            return f"{base} entre {intent.year_from} e {intent.year_to}."
        return f"{base} em ordem decrescente."
    if intent.task == "aggregate":
        tgt = intent.agg_field.replace("_", " ").lower() if intent.agg_field else "métrica"
        return f"Média de {tgt} conforme filtros." if (intent.agg or "AVG").lower()=="avg" else f"{(intent.agg or 'AGG').title()} de {tgt} conforme filtros."
    return "Consulta ao dataset local."
