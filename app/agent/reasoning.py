from __future__ import annotations
from typing import Dict, Any, Optional, Tuple, List
import re
import pandas as pd

from app.services.datastore import get_datastore


RANK_WORDS_PT = ["top", "mais vendidos", "ranking", "mais bem avaliados", "topo"]
RANK_WORDS_EN = ["top", "best selling", "ranking", "most sold", "best rated"]
AVG_WORDS = ["média", "media", "average", "mean", "nota", "rating"]
FRANCHISE_WORDS = ["franquia", "série", "serie", "franchise"]
DETAILS_HINTS = ["qual o ano", "quando saiu", "lançamento", "lancamento", "what year", "release year"]
OOS_HINTS = ["banana", "preço", "preco", "clima", "restaurante", "uber", "bitcoin", "dólar", "dolar", "imposto", "vacina"]


def _q(q: str) -> str:
    return (q or "").strip()


def _ql(q: str) -> str:
    return _q(q).lower()


def _year_from(q: str) -> Optional[int]:
    m = re.search(r"\b(19\d{2}|20\d{2}|21\d{2})\b", _q(q))
    return int(m.group(1)) if m else None


def _topn_from(q: str) -> Optional[int]:
    m = re.search(r"\btop\s*([0-9]{1,3})\b", _ql(q))
    return int(m.group(1)) if m else None


def _metric_from(q: str) -> str:
    ql = _ql(q)
    if any(w in ql for w in ["japão", "japao", "japan", "jp"]):
        return "JP_Sales"
    if any(w in ql for w in ["europa", "europe", "eu"]):
        return "EU_Sales"
    if any(w in ql for w in ["américa do norte", "america do norte", "na", "eua", "us", "usa"]):
        return "NA_Sales"
    if any(w in ql for w in ["outros", "other"]):
        return "Other_Sales"
    if any(w in ql for w in ["critic", "crítico", "critico", "crítica", "critica", "metacritic"]):
        return "Critic_Score"
    if any(w in ql for w in ["user", "usuário", "usuario", "userscore"]):
        return "User_Score"
    return "Global_Sales"


def _is_oos(q: str) -> bool:
    ql = _ql(q)
    return any(w in ql for w in OOS_HINTS)


def _intent_and_entities(q: str) -> Tuple[str, Dict[str, Any]]:
    ql = _ql(q)
    if _is_oos(q):
        return "oos", {}

    year = _year_from(q)

    if any(w in ql for w in DETAILS_HINTS):
        # naive: take text after 'de|do|da'
        name = None
        m = re.search(r"(?:de|do|da)\s+(.+)$", ql)
        if m:
            name = m.group(1).strip().strip("? ")
        return "title_lookup", {"name": name, "year": year}

    if any(w in ql for w in AVG_WORDS) and (
        any(w in ql for w in FRANCHISE_WORDS) or any(k in ql for k in ["zelda","mario","pokemon","final fantasy","call of duty"]) 
    ):
        fran = None
        for token in ["zelda","mario","pokemon","final fantasy","call of duty"]:
            if token in ql:
                fran = token
                break
        return "franchise_avg", {"franchise": fran, "year": year}

    if any(w in ql for w in RANK_WORDS_PT + RANK_WORDS_EN) or ("vendas" in ql or "vendidos" in ql):
        return "ranking", {"year": year}

    # default: ranking sensible
    return "ranking", {"year": year}


def _explain_pt(intent: str, q: str, metric: str, n: Optional[int], ents: Dict[str, Any], headline: Optional[str]) -> str:
    parts: List[str] = []
    if intent == "ranking":
        filt = f" em {ents['year']}" if ents.get("year") else ""
        parts.append(f"Entendi seu pedido: ranking de jogos{filt}.")
        parts.append(f"Vou usar vendas ({metric.replace('_',' ')}) e ordenar do maior para o menor. Mostro Top {n or 10}.")
    elif intent == "franchise_avg":
        f = ents.get("franchise") or "a franquia"
        parts.append(f"Entendi: médias de notas para a franquia {f.title()}.")
        parts.append("Vou calcular médias de Críticos (/100) e Usuários (/10), ignorando títulos sem nota.")
    elif intent == "title_lookup":
        nm = ents.get("name") or "o título"
        parts.append(f"Entendi: buscar detalhes sobre {nm}.")
    elif intent == "oos":
        parts.append("Este app responde perguntas sobre jogos de videogame (vendas, notas, franquias, anos, plataformas). Exemplos: 'Top 10 no Japão', 'média da franquia Zelda', 'mais vendidos em 2010'. Quer tentar uma dessas?")
    if headline:
        parts.append(f"Resultado: {headline}.")
    return " ".join(parts)


def reason(query: str) -> Dict[str, Any]:
    df = get_datastore().get_df()
    intent, ents = _intent_and_entities(query)
    metric = _metric_from(query)
    topn = _topn_from(query) or 10
    year = ents.get("year")

    # Build SQL per intent
    if intent == "oos":
        return {
            "intent": "oos",
            "entities": ents,
            "metric": metric,
            "top_n": None,
            "filters_sql": "",
            "group_by": None,
            "agg": None,
            "sql": "",
            "chart": {"type": "table", "x": "", "y": "", "desc": ""},
            "explanation_user": _explain_pt("oos", query, metric, None, ents, None),
        }

    if intent == "franchise_avg":
        fran = ents.get("franchise") or ""
        filters_sql = f"WHERE lower(Name) LIKE lower('%{fran}%')"
        sql = f"""
        WITH base AS (
          SELECT Name, Critic_Score, NULLIF(User_Score,'tbd') AS User_Score
          FROM games
          {filters_sql}
        )
        SELECT ROUND(AVG(Critic_Score),2) AS avg_critic_100,
               ROUND(AVG(CAST(User_Score AS FLOAT)),2) AS avg_user_10,
               COUNT(*) AS titles
        FROM base
        """
        return {
            "intent": "franchise_avg",
            "entities": ents,
            "metric": "User_Score",
            "top_n": None,
            "filters_sql": filters_sql,
            "group_by": None,
            "agg": "AVG",
            "sql": sql.strip(),
            "chart": {"type": "table", "x": "Name", "y": "User_Score", "desc": "Médias de notas"},
            "explanation_user": _explain_pt("franchise_avg", query, metric, None, ents, None),
        }

    if intent == "ranking":
        where = f"WHERE CAST(Year_of_Release AS INT) = {int(year)}" if year else ""
        sql = f"""
        SELECT Name, CAST(Year_of_Release AS INT) AS year, {metric}
        FROM games
        {where}
        ORDER BY {metric} DESC
        LIMIT {topn}
        """
        desc = {
            "Global_Sales": "Vendas Globais (milhões)",
            "JP_Sales": "Vendas no Japão (milhões)",
            "EU_Sales": "Vendas na Europa (milhões)",
            "NA_Sales": "Vendas na América do Norte (milhões)",
        }.get(metric, metric)
        return {
            "intent": "ranking",
            "entities": ents,
            "metric": metric,
            "top_n": topn,
            "filters_sql": where,
            "group_by": None,
            "agg": None,
            "sql": sql.strip(),
            "chart": {"type": "bar", "x": "Name", "y": metric, "desc": desc},
            "explanation_user": _explain_pt("ranking", query, metric, topn, ents, None),
        }

    # fallback details to table search
    if intent == "title_lookup":
        name = ents.get("name") or ""
        where = f"WHERE lower(Name) LIKE lower('%{name}%')" if name else ""
        sql = f"SELECT Name, Platform, CAST(Year_of_Release AS INT) AS year, Global_Sales, Critic_Score, User_Score FROM games {where} ORDER BY Global_Sales DESC LIMIT 20"
        return {
            "intent": "title_lookup",
            "entities": ents,
            "metric": "Global_Sales",
            "top_n": None,
            "filters_sql": where,
            "group_by": None,
            "agg": None,
            "sql": sql,
            "chart": {"type": "table", "x": "Name", "y": "Global_Sales", "desc": ""},
            "explanation_user": _explain_pt("title_lookup", query, metric, None, ents, None),
        }

    # default safe
    return {
        "intent": "ranking",
        "entities": ents,
        "metric": metric,
        "top_n": topn,
        "filters_sql": "",
        "group_by": None,
        "agg": None,
        "sql": "SELECT Name, CAST(Year_of_Release AS INT) AS year, Global_Sales FROM games ORDER BY Global_Sales DESC LIMIT 10",
        "chart": {"type": "bar", "x": "Name", "y": "Global_Sales", "desc": "Vendas globais"},
        "explanation_user": _explain_pt("ranking", query, metric, topn, ents, None),
    }


