from __future__ import annotations
from typing import Dict, Any, Optional, List
import re
import traceback

from app.services.agents import sql_agent, nlg_agent


class ReasoningTrace:
    def __init__(self) -> None:
        self.steps: List[Dict[str, Any]] = []

    def add(self, step: str, data: Any | None = None) -> None:
        item: Dict[str, Any] = {"step": step}
        if data is not None:
            item["data"] = data
        self.steps.append(item)

    def dump(self) -> List[Dict[str, Any]]:
        return self.steps


REGION_TOKENS: Dict[str, List[str]] = {
    "jp": [" jap", " japão", " japao", " no japao", " no japão", " jp "],
    "eu": [" europa", " europe ", " na europa", " eu "],
    "na": [" américa do norte", " america do norte", " north america"],
    "other": [" outros ", " other "]
}


def _detect_metric(ql: str) -> Optional[str]:
    ql2 = f" {ql} "
    for key, toks in REGION_TOKENS.items():
        if any(t in ql2 for t in toks):
            return key
    if any(t in ql for t in ["crític", "critic", "metacritic"]):
        return "critic"
    if any(t in ql for t in ["usuár", "user"]):
        return "user"
    return None


def _extract_year(text: str) -> Optional[int]:
    m = re.search(r"\b(19\d{2}|20\d{2}|21\d{2})\b", text)
    return int(m.group(1)) if m else None


def _extract_title_for_details(ql: str) -> Optional[str]:
    m = re.search(r"(?:do\s*jogo|jogo)\s+(.+)", ql)
    if m:
        return m.group(1).strip().strip("?!. ")
    return None


def _is_details_year(ql: str) -> bool:
    return any(p in ql for p in ["qual ano", "que ano", "quando", "em que ano", "em qual ano", "what year"]) and ("jogo" in ql or "game" in ql)


def classify(q: str) -> Dict[str, Any]:
    ql = q.lower().strip()

    # out-of-domain quick tokens
    if any(tok in ql for tok in ["banana", "preço", "preco", "clima", "restaurante", "uber", "bitcoin", "dólar", "dolar", "imposto", "vacina"]):
        return {"intent": "oob", "metric": None, "topn": None,
                "filters": {"year": None, "platform": None, "genre": None, "publisher": None, "developer": None, "franchise": None, "name": None}}

    # details (year) intent
    if _is_details_year(ql):
        title = _extract_title_for_details(ql)
        if title:
            return {"intent": "details", "metric": None, "topn": None,
                    "filters": {"year": None, "platform": None, "genre": None, "publisher": None, "developer": None, "franchise": None, "name": title}}

    # franchise averages
    if any(t in ql for t in ["média", "media", "average"]) and ("franquia" in ql or any(f in ql for f in ["zelda", "mario", "pokemon", "final fantasy", "call of duty"])):
        fam = None
        for f in ["zelda", "mario", "pokemon", "final fantasy", "call of duty"]:
            if f in ql:
                fam = f
                break
        return {"intent": "franchise_avg", "metric": "user", "topn": None,
                "filters": {"year": None, "platform": None, "genre": None, "publisher": None, "developer": None, "franchise": fam, "name": None}}

    # rankings default (metric by region if present)
    metric = _detect_metric(ql) or "global"
    return {"intent": "rankings", "metric": metric, "topn": 10,
            "filters": {"year": _extract_year(q), "platform": None, "genre": None, "publisher": None, "developer": None, "franchise": None, "name": None}}


def route_and_execute(q: str) -> Dict[str, Any]:
    trace = ReasoningTrace()
    try:
        plan = classify(q)
        trace.add("intent_detected", plan)

        if plan.get("intent") == "oob":
            nl = nlg_agent.summarize_out_of_domain(q)
            return {"kind": "out_of_domain", "nl": nl, "columns": [], "rows": [], "rows_dict": [], "chart": None, "reasoning": trace.dump()}

        # Ask SQL agent
        res = sql_agent.llm_build_sql_and_run(q, plan)
        trace.add("sql_built", {"metric": (res.get("meta") or {}).get("metric_label")})
        trace.add("sql_executed", {"rows": len(res.get("rows", []))})

        # details intent → not found
        if plan.get("intent") == "details":
            if len(res.get("rows", [])) == 0:
                title = (plan.get("filters") or {}).get("name")
                trace.add("not_found_check", {"matched_rows": 0, "name": title})
                nl = nlg_agent.summarize_not_found_title(title)
                return {"kind": "not_found", "nl": nl, "columns": [], "rows": [], "rows_dict": [], "chart": None, "reasoning": trace.dump()}
            nl = nlg_agent.summarize_details(q, {"kind": "details", **plan}, res.get("rows_dict", []))
            trace.add("nlg", {"mode": "concise"})
            return {"kind": "details", "nl": nl, **res, "reasoning": trace.dump()}

        # other intents
        metric_label = (res.get("meta") or {}).get("metric_label", "Global_Sales")
        nl = nlg_agent.summarize_in_domain(q, {"kind": plan.get("intent"), **plan}, res.get("rows_dict", []), metric_label, extra_meta=res.get("meta"))
        trace.add("nlg", {"mode": "concise"})
        return {"kind": plan.get("intent"), "nl": nl, **res, "reasoning": trace.dump()}

    except Exception as e:
        trace.add("exception", {"type": type(e).__name__, "message": str(e), "tb": traceback.format_exc()})
        return {"kind": "error", "nl": "Ocorreu um erro ao processar sua pergunta.", "columns": [], "rows": [], "rows_dict": [], "chart": None, "reasoning": trace.dump()}


