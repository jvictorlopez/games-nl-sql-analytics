import json
import time
from typing import Iterable, Dict, Any
import re

from app.core.config import get_settings
from app.services.datastore import get_datastore
from app.agents.router import route_query
from app.agents.presence import check_presence
from app.agents.sqlgen import sql_with_plot, run_duckdb_sql
from app.agents.websearch import web_fallback

S = get_settings()

def _jline(event: str, **kwargs) -> bytes:
    payload: Dict[str, Any] = {"event": event, **kwargs}
    return (json.dumps(payload) + "\n").encode("utf-8")

_GAMING_MARKERS = [
    "game", "jogo", "console", "platform", "plataforma", "vendas", "sales",
    "nintendo", "sony", "sega", "microsoft", "xbox", "playstation", "wii",
    "ds", "gb", "nes", "snes", "ps2", "ps3", "ps4", "ps5", "switch", "steam",
    "critic", "usuário", "score", "genre", "publisher", "developer"
]

def _is_out_of_domain(q: str) -> bool:
    ql = q.lower()
    if any(m in ql for m in _GAMING_MARKERS):
        return False
    # very rough heuristic: if it's clearly about finance/news/other topics and not games
    non_gaming = ["stocks", "bitcoin", "weather", "traffic", "movie", "restaurant", "football", "politics", "economy"]
    return any(m in ql for m in non_gaming)

def orchestrate_stream(q: str) -> Iterable[bytes]:
    df = get_datastore().get_df()
    t0 = time.time()

    yield _jline("run_started", message="🚀 LUXOR AGI multi-agent pipeline running", query=q)

    # Domain guard
    if _is_out_of_domain(q):
        yield _jline(
            "bounce_back",
            message="Este assistente é focado em GAMES. O dataset cobre 1980–2020, com rankings por continentes, vendas e notas.",
        )
        yield _jline("done", elapsed=round(time.time() - t0, 3))
        return

    # Route
    yield _jline("route_start", message="Orquestrador analisando a intenção…")
    decision = route_query(q)
    yield _jline("route_decision", decision=decision)

    # Presence only for specific-title-like queries
    matches = check_presence(q, df)
    if matches:
        yield _jline("presence_result", present_matches=matches)

    if decision == "dataset":
        # SQL gen
        yield _jline("sqlgen_start", message="Gerando SQL para DuckDB…", llm_used=bool(S.OPENAI_API_KEY))
        sql, plot_hint = sql_with_plot(q, df)
        yield _jline("sqlgen_result", sql=sql)

        # Guard
        if not sql.strip().lower().startswith("select"):
            yield _jline("error", message="A consulta gerada não é SELECT. Abortando por segurança.")
            yield _jline("done", elapsed=round(time.time() - t0, 3))
            return

        # Execute
        yield _jline("exec_start", message="Executando no DuckDB…")
        try:
            out_df = run_duckdb_sql(sql, "games", df)
            preview = out_df.head(50).to_dict(orient="records")
            cols = list(out_df.columns)
            yield _jline("exec_result", columns=cols, rows=preview, rowcount=len(out_df), plot_hint=plot_hint)
        except Exception as e:
            yield _jline("error", message=f"Erro de execução SQL: {str(e)}")
            yield _jline("done", elapsed=round(time.time() - t0, 3))
            return

        yield _jline("done", mode="dataset", elapsed=round(time.time() - t0, 3))
        return

    # Web fallback
    yield _jline("web_start", message="Pergunta parece exigir dados externos…")
    if S.ALLOW_WEB:
        rows = web_fallback(q).get("rows", [])
        yield _jline("web_result", rows=rows)
        yield _jline("done", mode="web", elapsed=round(time.time() - t0, 3))
    else:
        yield _jline("web_disabled", message="Busca externa desabilitada por configuração.")
        yield _jline("done", mode="dataset", elapsed=round(time.time() - t0, 3))

