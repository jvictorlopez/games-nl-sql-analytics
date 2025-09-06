from __future__ import annotations
from typing import Any, Dict, List, Optional
from app.core.llm import chat

# Natural language agent — replies in the user’s language, *after* data retrieval.
NLG_SYS_PROMPT = """\
You are the NLG layer for a Games Analytics app. You receive the user's question,
the plan (kind/metric/filters), and the already-computed rows (small JSON).
Write ONE concise answer in the user's language (Portuguese or English).
Never invent numbers; only use provided rows/meta.

Dataset columns available in rows:
['Rank','Name','year','Global_Sales','NA_Sales','EU_Sales','JP_Sales','Other_Sales',
 'Critic_Score','Critic_Count','User_Score','User_Count', ...]

Guidelines:
- For rankings: say what was ranked (metric + filters like year) and list the top N by name with the metric.
  Prefer a short sentence followed by a compact comma-separated list (or bullets if N>10).
- For franchise_avg: report weighted averages if provided in meta.franchise_weighted.
- For summary: mention total titles, period, global sales sum, and average scores.
- For details: summarize briefly and highlight a few key fields.
- Keep it tight, professional, and friendly.
 - Rows without scores (both Critic_Score and User_Score null) may be omitted to keep the view concise; assume this when summarizing.
"""


def _render_list(rows: List[Dict[str, Any]], metric_col: str, limit: Optional[int]) -> str:
    n = limit or len(rows)
    parts: List[str] = []
    for r in rows[:n]:
        name = r.get("Name")
        yr = r.get("year")
        val = r.get(metric_col)
        if val is None:
            parts.append(f"{name} ({yr})")
        else:
            try:
                parts.append(f"{name} ({yr}) – {float(val):.2f}")
            except Exception:
                parts.append(f"{name} ({yr}) – {val}")
    return ", ".join(parts)


def _fallback_rankings(question: str, plan: Dict[str, Any], rows: List[Dict[str, Any]], metric_col: str) -> str:
    year = (plan.get("filters") or {}).get("year")
    when = f" em {year}" if year else ""
    n = plan.get("topn") or len(rows)
    listing = _render_list(rows, metric_col, n)
    return f"Top {n} por {metric_col}{when}: {listing}."


def _fallback_franchise_avg(question: str, plan: Dict[str, Any], rows: List[Dict[str, Any]], meta: Dict[str, Any]) -> str:
    fam = (plan.get("filters") or {}).get("franchise") or "a franquia"
    w = (meta or {}).get("franchise_weighted", {})
    cwa = w.get("critic_wavg")
    uwa = w.get("user_wavg")
    cc  = w.get("critic_count_sum")
    uc  = w.get("user_count_sum")
    tt  = w.get("total_titles")
    parts = []
    if uwa is not None: parts.append(f"média ponderada dos usuários {float(uwa):.2f}")
    if cwa is not None: parts.append(f"média ponderada da crítica {float(cwa):.2f}")
    core = " e ".join(parts) if parts else "médias não disponíveis"
    extra = f" (n usuários={uc}, n críticas={cc}, títulos considerados={tt})" if any([uc,cc,tt]) else ""
    return f"Para {fam.title()}, {core}{extra}."


def summarize_in_domain(question: str, plan: Dict[str, Any], rows: List[Dict[str, Any]], metric_col: str, extra_meta: Optional[Dict[str, Any]] = None) -> str:
    payload = {
        "question": question,
        "kind": plan.get("kind"),
        "metric": metric_col,
        "filters": plan.get("filters"),
        "topn": plan.get("topn"),
        "rows_preview": rows[: min(12, len(rows))],
        "meta": extra_meta or {},
    }
    out = chat(
        [{"role": "system", "content": NLG_SYS_PROMPT},
         {"role": "user", "content": f"Resume naturalmente (1–3 frases). Use os dados abaixo sem inventar nada:\n{payload}"}],
        temperature=0.2,
    )
    if out:
        return out.strip()

    # deterministic fallbacks
    k = plan.get("kind")
    if k == "franchise_avg":
        return _fallback_franchise_avg(question, plan, rows, extra_meta or {})
    return _fallback_rankings(question, plan, rows, metric_col)


def summarize_out_of_domain(question: str) -> str:
    out = chat(
        [{"role": "system", "content": NLG_SYS_PROMPT},
         {"role": "user", "content": f"Este pedido está fora do escopo do dataset (games). Explique educadamente e sugira exemplos. Pergunta: {question}"}],
        temperature=0.2,
    )
    if out:
        return out.strip()
    return ("Sua pergunta parece estar fora do escopo deste app (focado em dados de videogames). "
            "Tente: 'Top 10 vendas globais em 2010', 'Top 10 no Japão por User_Score', 'Média de nota da franquia Zelda'.")


def summarize_not_found(question: str, filters: Dict[str, Any], suggestions: List[str]) -> str:
    term = (filters or {}).get("name") or (filters or {}).get("franchise") or "o título"
    if suggestions:
        sug = "; ".join(suggestions[:5])
        return (f"Não encontrei '{term}' na base. Tente um dos títulos parecidos: {sug}. "
                f"Você também pode refazer a busca com outro nome ou parte do nome.")
    return (f"Não encontrei '{term}' na base. Se quiser, refaça a busca com outra grafia "
            f"ou peça um ranking/estatística geral.")


def summarize_franchise_total_sales(question: str, filters: Dict[str, Any], rows: List[Dict[str, Any]]) -> str:
    fam = (filters or {}).get("franchise") or "a franquia"
    if not rows:
        return f"Não consegui calcular as vendas totais de {fam.title()}."
    r = rows[0]
    try:
        g = float(r.get("Global_Sales") or 0)
        na = float(r.get("NA_Sales") or 0); eu = float(r.get("EU_Sales") or 0)
        jp = float(r.get("JP_Sales") or 0); ot = float(r.get("Other_Sales") or 0)
        t  = int(r.get("Titles") or 0)
    except Exception:
        g = na = eu = jp = ot = 0.0; t = 0
    return (f"A franquia {fam.title()} soma {g:.2f} milhões globalmente "
            f"(NA {na:.2f}, EU {eu:.2f}, JP {jp:.2f}, Outros {ot:.2f}; títulos considerados={t}).")
