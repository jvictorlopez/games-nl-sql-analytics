from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from fastapi.responses import StreamingResponse
from app.agents.orchestrator import orchestrate_stream

from app.core.config import get_settings
from app.services.datastore import get_datastore
from app.agents.router import route_query
from app.agents.presence import check_presence
from app.agents.sqlgen import generate_sql, run_duckdb_sql
from app.agents.websearch import web_fallback

router = APIRouter(prefix="/ask", tags=["ask"]) 

class AskOut(BaseModel):
    mode: str  # "dataset" or "web"
    notice: Optional[str] = None
    sql: Optional[str] = None
    rows: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    present_matches: Optional[List[str]] = None

@router.get("")
def ask(q: str = Query(..., description="Natural language question")) -> Dict[str, Any]:
    """
    Multi-agent entrypoint. For now, web is off by default.
    """
    from app.services.agents.orchestrator import route_query
    from app.services.agents.sql_agent import build_sql, run_sql
    
    decision = route_query(q)

    if decision["route"] == "bounce":
        return {
            "route": "bounce",
            "mode": "dataset",
            "notice": decision["reason"],
            "message": decision["reason"],
            "columns": [],
            "rows": [],
            "chart": None,
        }

    if decision["route"] == "sql":
        intent = decision["intent"]
        sql, chart = build_sql(intent)
        cols, rows = run_sql(sql)
        return {
            "route": "sql",
            "mode": "dataset",
            "notice": decision.get("reason", ""),
            "message": decision.get("reason", ""),
            "sql": sql,
            "columns": cols,
            "rows": rows,
            "chart": chart,
        }

    # If someday web is enabled, branch here. For now fallback to bounce.
    return {
        "route": "bounce",
        "mode": "dataset",
        "notice": "Busca externa desabilitada.",
        "message": "Busca externa desabilitada.",
        "columns": [],
        "rows": [],
        "chart": None,
    }

@router.get("/stream")
def ask_stream(q: str):
    """
    Streams agent events as JSONL.
    """
    stream = orchestrate_stream(q)
    return StreamingResponse(stream, media_type="application/json")


