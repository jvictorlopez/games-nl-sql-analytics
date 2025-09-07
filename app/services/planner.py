from __future__ import annotations
from typing import Dict, Any
import re, json
from app.core.llm import chat  # existing LLM helper

COLUMNS = ['Name','Platform','Year_of_Release','Genre','Publisher','NA_Sales','EU_Sales','JP_Sales',
           'Other_Sales','Global_Sales','Critic_Score','Critic_Count','User_Score','User_Count','Developer','Rating']

OOB_HINTS = ["banana","preço","preco","clima","restaurante","uber","bitcoin","dólar","dolar","imposto","vacina"]

def _is_pt(q:str)->bool:
    ql=(q or "").lower()
    return any(w in ql for w in ["qual","quais","ano","venda","franquia","no","em","média","media"])

def _scope_check(q:str)->Dict[str,Any]:
    ql=(q or "").lower().strip()
    oob = any(h in ql for h in OOB_HINTS)
    if oob:
        msg_pt = "Sua pergunta parece fora do escopo do dataset de videogames."
        msg_en = "Your question seems out of scope for the videogame dataset."
        return {"in_scope": False, "message": msg_pt if _is_pt(q) else msg_en}
    return {"in_scope": True, "message": ""}

def _detect_year(q:str):
    m=re.search(r"\b(19\d{2}|20\d{2}|21\d{2})\b",(q or ""))
    return int(m.group(1)) if m else None

def _detect_metric(q:str)->str:
    ql=(q or "").lower()
    if "jap" in ql: return "JP_Sales"
    if "europ" in ql or " eu " in ql: return "EU_Sales"
    if "américa do norte" in ql or "america do norte" in ql or " na " in ql or "north america" in ql: return "NA_Sales"
    return "Global_Sales"

# ---- Chain-1 prompt (reasoning + plan JSON) ----
PLANNER_SYS = """
Você é o PLANEJADOR de um app de analytics de videogames.
1) Escreva 2–4 frases NATURAIS (PT se a pergunta for PT, EN senão) explicando:
   • o que você entendeu, • ano/região se houver, • se buscará título/franquia por aproximação (LIKE),
   • que enviará um plano de execução ao Executor. NÃO mencione intents, colunas, SQL.
2) Em seguida, produza APENAS 1 JSON com o PLANO (sem SQL), formato:
{
 "expected_answer": "kpi|ranking|table|trend|franchise_avg|sum|oob",
 "select": [{"expr":"year|Name|..."}],
 "where":  [{"col":"Name","op":"ilike","val":"%grand theft auto v%"}],
 "group_by": ["lower(Name)"],
 "order_by": [{"expr":"metric","dir":"DESC"}],
 "limit": 10,
 "aggregates": [{"fn":"avg","col":"User_Score","alias":"avg_user"}],
 "meta": {"metric":"Global_Sales|NA_Sales|EU_Sales|JP_Sales|Other_Sales|User_Score|Critic_Score",
          "year": null|int, "topn": null|int,
          "entity": {"title_like": null|string, "franchise_like": null|string},
          "need_graph": true|false}
}
Regras: se for ranking, usar meta.topn; para título/termo, use WHERE com ILIKE; para média de franquia,
agregue User_Score/Critic_Score ignorando 'tbd' (o Executor cuidará disso).
Não inclua SQL. Escreva o texto natural ANTES do JSON.
"""

def plan(question: str) -> Dict[str, Any]:
    q=(question or "").strip()
    sc=_scope_check(q)
    if not sc["in_scope"]:
        return {"oob": True, "reasoning": sc["message"], "plan": {"expected_answer":"oob","meta":{}}}

    # defaults from heuristics
    defaults = {"metric": _detect_metric(q), "year": _detect_year(q), "topn": None}

    # Call LLM once to get reasoning text + plan JSON
    out = chat([{"role":"system","content":PLANNER_SYS},
                {"role":"user","content":q}], temperature=0.2) or ""
    # split reasoning and json
    m = re.search(r"\{[\s\S]*\}\s*$", out.strip())
    reasoning = out.strip()
    plan_json: Dict[str,Any]={}
    if m:
        reasoning = out[:m.start()].strip()
        try: plan_json = json.loads(m.group(0))
        except Exception: plan_json = {}

    # fill defaults safely
    plan_json.setdefault("meta",{})
    plan_json["meta"].setdefault("metric", defaults["metric"])
    if plan_json["meta"].get("year") is None and defaults["year"] is not None:
        plan_json["meta"]["year"]=defaults["year"]
    if plan_json.get("expected_answer")=="ranking":
        plan_json["limit"]= plan_json.get("limit") or plan_json["meta"].get("topn") or 10

    # Deterministic fallback when model didn't produce a plan
    if not plan_json.get("expected_answer"):
        ql = q.lower()
        year = defaults["year"]
        metric = defaults["metric"]
        # Franchise averages
        if ("média" in ql or "media" in ql or "average" in ql or "nota" in ql) and ("franquia" in ql or "franchise" in ql or any(t in ql for t in ["zelda","mario","pokemon","final fantasy","call of duty"])):
            fran = None
            for t in ["zelda","mario","pokemon","final fantasy","call of duty"]:
                if t in ql:
                    fran = t
                    break
            plan_json = {
                "expected_answer": "franchise_avg",
                "select": [],
                "where": ([{"col":"Name","op":"ilike","val": f"%{fran}%"}] if fran else []),
                "group_by": [],
                "order_by": [],
                "limit": None,
                "aggregates": [{"fn":"avg","col":"User_Score","alias":"avg_user"}],
                "meta": {"metric": "User_Score", "year": year, "topn": None, "entity": {"franchise_like": fran}}
            }
            if not reasoning:
                reasoning = "Vou calcular as médias de notas para a franquia solicitada, ignorando títulos sem nota."
        # Details/year style → mark not_found to match legacy behavior
        elif any(w in ql for w in ["qual o ano","quando saiu","lançamento","lancamento","what year","release year"]):
            # leave to executor/table; orchestrator/NLG handles friendly NL
            plan_json = {"expected_answer": "table", "where": [], "meta": {"metric": metric, "year": year}}
            if not reasoning:
                reasoning = "Vou buscar pelo título mencionado aproximando por nome."
        else:
            # Default ranking
            topn = None
            m = re.search(r"\btop\s*(\d{1,3})\b", ql)
            if m:
                topn = int(m.group(1))
            plan_json = {
                "expected_answer": "ranking",
                "select": [{"expr":"Name"},{"expr":"year"}],
                "where": [],
                "group_by": [],
                "order_by": [{"expr":"metric","dir":"DESC"}],
                "limit": topn or 10,
                "aggregates": [],
                "meta": {"metric": metric, "year": year, "topn": topn or 10}
            }
            if year is not None:
                plan_json["where"].append({"col":"Year_of_Release","op":"eq","val": year})
            if not reasoning:
                reasoning = "Vou montar um ranking com a métrica e filtros deduzidos da sua pergunta."

    return {"oob": False, "reasoning": reasoning, "plan": plan_json}


