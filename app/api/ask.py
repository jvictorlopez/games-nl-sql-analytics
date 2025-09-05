from __future__ import annotations
from fastapi import APIRouter, Query
from app.services.agents.orchestrator import route_and_execute

router = APIRouter(prefix="/ask", tags=["ask"]) 

@router.get("")
def ask(q: str = Query(..., description="Pergunta em linguagem natural")):
    return route_and_execute(q)


