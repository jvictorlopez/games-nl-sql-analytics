from __future__ import annotations
from typing import Dict, Any, Optional, List, Tuple
import json
import re
import traceback
import logging

from app.core.llm import chat
from app.services.datastore import get_datastore
from app.services.agents import sql_agent, nlg_agent
from rapidfuzz import process, fuzz

logger = logging.getLogger("nl2sql")
logger.setLevel(logging.INFO)

# API mode (dataset-only by design)
MODE = "dataset"

# --- Keyword dictionaries (pt/en) ---
RANK_WORDS = {
    "pt": ["top", "mais vendidos", "mais jogados", "ranking", "mais bem avaliados", "topo"],
    "en": ["top", "best selling", "ranking", "most sold", "best rated"],
}

ME_SALES = {
    "global": ["global", "mundo", "mundial", "vendas"],
    "na": ["na", "north america", "américa do norte", "america do norte", "eua", "us", "usa"],
    "eu": ["europa", "europe", "eu"],
    "jp": ["japão", "japao", "japan", "jp"],
    "other": ["outros", "other"],
}
ME_SCORES = {
    "critic": ["crítico", "critico", "crítica", "critica", "metacritic", "crítica média", "critic"],
    "user": ["usuário", "usuario", "user", "userscore", "nota de usuário", "nota de usuarios"],
}

FRANCHISE_WORDS = ["franquia", "série", "serie", "franchise"]
AVG_WORDS = ["média", "media", "average"]
DETAILS_WORDS = ["qual o ano", "quando saiu", "lançamento", "lancamento", "release year", "what year"]
SUMMARY_WORDS = ["panorama", "resumo", "overview", "sumário", "sumario", "estatísticas", "statistics"]
OOB_HINTS = ["banana", "preço", "preco", "clima", "restaurante", "uber", "bitcoin", "dólar", "dolar", "imposto", "vacina"]

COLS = [
    "Name","Platform","Year_of_Release","Genre","Publisher","NA_Sales","EU_Sales",
    "JP_Sales","Other_Sales","Global_Sales","Critic_Score","Critic_Count","User_Score",
    "User_Count","Developer","Rating"
]

# --- NEW: NL-first Reasoner prompt (rationale + structured plan)
REASONER_SYS_PROMPT = """
Você é o Reasoner (somente raciocínio natural) para uma aplicação NL→SQL de videogames.
1) Leia a pergunta do usuário.
2) Pense em voz alta (português) em poucos passos, com base nas colunas do dataset:
   ['Name','Platform','Year_of_Release','Genre','Publisher','NA_Sales','EU_Sales',
    'JP_Sales','Other_Sales','Global_Sales','Critic_Score','Critic_Count',
    'User_Score','User_Count','Developer','Rating'].
   - Se for ranking regional, identifique: NA/EU/JP/Other/Global.
   - Se houver ano, capture o inteiro do ano.
   - Se for sobre uma franquia (ex.: 'Zelda', 'Mario'), capture o termo cru.
   - Se for um título específico, procure no Name (case-insensitive).
   - Se estiver fora do escopo (banana, dólar etc.), marque como OOD.
3) Só então produza um PLANO JSON com o formato estrito:
   {
     "intent": "rankings"|"franchise_avg"|"summary"|"details"|"total_franchise_sales"|"not_found"|"oob",
     "metric": "global|na|eu|jp|other|critic|user|combo|null",
     "topn": int|null,
     "filters": {
       "year": int|null,
       "platform": string|null,
       "genre": string|null,
       "publisher": string|null,
       "developer": string|null,
       "franchise": string|null,
       "name": string|null
     }
   }
REGRAS:
- 'total_franchise_sales' quando pedir "quantas vendas no total da franquia X".
- Se título não existir no dataset, use 'not_found' com name preenchido.
- Não devolva nenhum outro texto junto do JSON. O raciocínio vem separado.
"""


def _extract_year(text: str) -> Optional[int]:
    m = re.search(r"\b(19\d{2}|20\d{2}|21\d{2})\b", text)
    return int(m.group(1)) if m else None


def _find_titles_like(term: str, limit: int = 10) -> List[str]:
    if not term:
        return []
    try:
        import duckdb
        df = get_datastore().get_df()
        con = duckdb.connect()
        con.register("games", df)
        esc = term.replace("'", "''")
        rows = con.execute(
            f"SELECT DISTINCT Name FROM games WHERE lower(Name) LIKE lower('%{esc}%') LIMIT {int(limit)}"
        ).fetchall()
        con.close()
        return [r[0] for r in rows]
    except Exception:
        return []


def _json_from_llm_output(txt: str) -> Dict[str, Any] | None:
    if not txt:
        return None
    s = txt.strip()
    s = re.sub(r"^```(json)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)
    try:
        return json.loads(s)
    except Exception:
        m = re.search(r"\{[\s\S]*\}", s)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                return None
    return None


def _q(q: str) -> str:
    return (q or "").strip().lower()


def _contains_any(ql: str, words: List[str]) -> bool:
    return any(w in ql for w in words)


def _extract_topn(q: str) -> Optional[int]:
    m = re.search(r"\btop[\s\-]*(\d{1,3})\b", q)
    return int(m.group(1)) if m else None


def _detect_metric(q: str) -> str:
    ql = _q(q).lower()
    if _contains_any(ql, ME_SCORES["critic"]):
        return "critic"
    if _contains_any(ql, ME_SCORES["user"]):
        return "user"
    # lenient region substrings
    if "jap" in ql:
        return "jp"
    if "europ" in ql:
        return "eu"
    for key, toks in ME_SALES.items():
        if _contains_any(ql, toks):
            return key
    return "global"


def _format_reasoning(q: str, intent: str, metric: Optional[str], topn: Optional[int], filters: Dict[str, Any], title_check: Optional[Dict[str, Any]] = None) -> str:
    parts: List[str] = [f"Pergunta entendida: “{q}”.", f"Intenção: {intent}."]
    if metric:
        parts.append(f"Métrica: {metric}.")
    if topn:
        parts.append(f"TopN: {topn}.")
    if filters.get("year"):
        parts.append(f"Filtro de ano: {filters['year']}.")
    if title_check:
        if title_check.get("queried_name"):
            parts.append(f"Procurei pelo título: “{title_check['queried_name']}”.")
        if title_check.get("found"):
            parts.append("Título encontrado no dataset.")
        else:
            sugg = title_check.get("suggestions") or []
            if sugg:
                s = "; ".join([f"{n} ({int(sv)}%)" for n, sv in sugg])
                parts.append(f"Sugestões: {s}.")
    parts.append("Colunas consideradas: " + ", ".join(COLS))
    return " ".join(parts)


def _detect_intent_and_filters(q: str) -> Tuple[str, Dict[str, Any]]:
    ql = _q(q)
    if _contains_any(ql, OOB_HINTS):
        return "oob", {"year": None, "platform": None, "genre": None, "publisher": None,
                        "developer": None, "franchise": None, "name": None}
    year = _extract_year(ql)
    if _contains_any(ql, AVG_WORDS) and (_contains_any(ql, FRANCHISE_WORDS) or any(x in ql for x in ["zelda","mario","pokemon", "final fantasy", "call of duty"])):
        fran = None
        m = re.search(r"(franquia|s[ée]rie)\s+([a-z0-9\-\:\'\!\s]+)", ql)
        if m:
            fran = m.group(2).strip()
        else:
            for candidate in ["zelda","mario","pokemon","final fantasy","call of duty"]:
                if candidate in ql:
                    fran = candidate
                    break
        return "franchise_avg", {"year": None, "platform": None, "genre": None, "publisher": None,
                                   "developer": None, "franchise": fran, "name": None}
    if _contains_any(ql, DETAILS_WORDS) or ("ano" in ql and _contains_any(ql, ["jogo","zelda","mario"])):
        name = None
        m = re.search(r"(?:de|do|da)\s+(.+)$", ql)
        if m:
            name = m.group(1).strip().strip("? ")
        return "details", {"year": None, "platform": None, "genre": None, "publisher": None,
                             "developer": None, "franchise": None, "name": name}
    if _contains_any(ql, SUMMARY_WORDS):
        return "summary", {"year": None, "platform": None, "genre": None, "publisher": None,
                             "developer": None, "franchise": None, "name": None}
    if _contains_any(ql, RANK_WORDS["pt"] + RANK_WORDS["en"]) or ("vendidos" in ql or "vendas" in ql):
        return "rankings", {"year": year, "platform": None, "genre": None, "publisher": None,
                              "developer": None, "franchise": None, "name": None}
    return "unknown", {"year": year, "platform": None, "genre": None, "publisher": None,
                         "developer": None, "franchise": None, "name": None}


def reason_and_plan(q: str) -> Tuple[Dict[str, Any], str]:
    ql = _q(q)
    intent, filters = _detect_intent_and_filters(ql)
    metric = _detect_metric(ql)
    topn = _extract_topn(ql) or 10

    # If still unknown, optionally ask a tiny LLM classifier (no title probing here)
    if intent == "unknown":
        ORCH_SYS_PROMPT = (
            "You classify a user question about a videogame dataset into a strict plan JSON. "
            "Only choose among: rankings | franchise_avg | summary | details | oob. "
            "Return ONLY JSON with: {\"intent\": \"...\", \"metric\": \"...\", \"topn\": int|null, \"filters\": {...}}"
        )
        try:
            out = chat([{ "role": "system", "content": ORCH_SYS_PROMPT }, { "role": "user", "content": q }], temperature=0.0)
            j = json.loads(out) if out else {}
            intent = j.get("intent") or intent
            metric = j.get("metric") or metric or "global"
            topn = j.get("topn") or topn
            jf = j.get("filters") or {}
            for k in filters:
                if k in jf and jf[k] is not None:
                    filters[k] = jf[k]
        except Exception:
            # keep deterministic plan
            pass

    # Only title check for details/franchise
    title_check = None
    if intent in ("details", "franchise_avg"):
        query_name = filters.get("name") or filters.get("franchise")
        if query_name:
            choices = get_datastore().get_df()["Name"].dropna().astype(str).unique().tolist()
            hits = process.extract(query_name, choices, scorer=fuzz.WRatio, limit=5)
            found = bool(hits and hits[0][1] >= 92)
            title_check = {"queried_name": query_name, "found": found, "suggestions": [(h[0], float(h[1])) for h in hits] if not found else []}
            if intent == "details" and not found:
                reasoning = _format_reasoning(q, "not_found", None, None, filters, title_check)
                return {"intent": "not_found", "metric": None, "topn": None, "filters": filters}, reasoning

    plan = {"intent": intent, "metric": metric, "topn": topn if intent == "rankings" else None, "filters": filters}
    reasoning = _format_reasoning(q, intent, metric, topn, filters, title_check)
    return plan, reasoning


def _ensure_presence_or_mark_not_found(plan: Dict[str, Any]) -> Dict[str, Any]:
    intent = (plan.get("intent") or "").lower()
    filters = plan.get("filters") or {}
    name = (filters.get("name") or "").strip()
    franchise = (filters.get("franchise") or "").strip()
    needs_title = intent in ("details","title_year")
    needs_franchise = intent in ("franchise_avg","franchise_total_sales")
    if not (needs_title or needs_franchise):
        return plan
    term = name if needs_title else franchise
    hits = _find_titles_like(term, limit=1)
    if not hits:
        plan["intent"] = "not_found"
        plan["_suggestions"] = _find_titles_like(term, limit=8)
    return plan


def route_and_execute(q: str) -> Dict[str, Any]:
    try:
        plan, reason_text = reason_and_plan(q)
        # Defensive: handle legacy order (reason_text, plan)
        if isinstance(plan, str) and isinstance(reason_text, dict):
            reason_text, plan = plan, reason_text
        intent = (plan.get("intent") or "rankings").lower()
        logger.info("ASK q=%r plan=%s", q, plan)

        if intent == "oob":
            return {"mode": MODE, "route":"bounce", "kind": "out_of_domain", "nl": nlg_agent.summarize_out_of_domain(q), "columns": [], "rows": [], "chart": None, "reasoning": reason_text}

        if intent == "not_found":
            return {"mode": MODE, "route":"not_found", "kind": "not_found", "nl": "Não encontrei o título na base. Tente outra grafia ou peça um ranking/estatística geral.", "columns": [], "rows": [], "chart": None, "reasoning": reason_text}

        if intent in ("franchise_total_sales", "total_franchise_sales"):
            res = sql_agent.run_franchise_total_sales(plan)
            nl = nlg_agent.summarize_franchise_total_sales(q, plan.get("filters", {}), res.get("rows_dict", []))
            return {"mode": MODE, "route":"sql", "kind": "franchise_total_sales", "reasoning": reason_text, "nl": nl, **res}

        res = sql_agent.llm_build_sql_and_run(q, plan)
        metric_lbl = (res.get("meta") or {}).get("metric_label", "Global_Sales")
        nl = nlg_agent.summarize_in_domain(q, {"kind": intent, **plan}, res.get("rows_dict", []), metric_lbl)
        return {"mode": MODE, "route":"sql", "kind": intent, "reasoning": reason_text, "nl": nl, **res}

    except Exception as e:
        return {"mode": MODE, "route": "error", "kind": "error", "nl": "Ocorreu um erro ao processar sua pergunta.", "error": str(e), "columns": [], "rows": [], "chart": None}


