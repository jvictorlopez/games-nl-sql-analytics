from __future__ import annotations
from typing import Dict, Any

from app.services.nl.entity_normalizer import peek


def explain(question: str, signals: Dict[str, Any] | None = None) -> str:
    q = (question or "").strip()
    sig = signals or peek(q)
    # Keep it simple and deterministic; PT if query has diacritics/common PT words
    is_pt = any(w in q.lower() for w in ["qual", "quais", "vendas", "franquia", "ano", "no", "em", "média", "media"])
    region = sig.get("region") or "Global_Sales"
    yr = sig.get("year")
    if is_pt:
        base = f"Entendi seu pedido: {q}. "
        if yr:
            base += f"Vou considerar o ano {yr}. "
        reg_text = {
            "JP_Sales": "vendas no Japão",
            "EU_Sales": "vendas na Europa",
            "NA_Sales": "vendas na América do Norte",
            "Global_Sales": "vendas globais",
        }.get(region, "vendas globais")
        base += f"Usarei {reg_text} e campos como Nome, Plataforma e Ano para montar a resposta. "
        base += "Se algum título específico for mencionado, faço a busca pelo nome aproximado."
        return base
    else:
        base = f"I understood: {q}. "
        if yr:
            base += f"I'll filter to year {yr}. "
        reg_text = {
            "JP_Sales": "sales in Japan",
            "EU_Sales": "sales in Europe",
            "NA_Sales": "sales in North America",
            "Global_Sales": "global sales",
        }.get(region, "global sales")
        base += f"I'll use {reg_text} and fields like Name, Platform and Year to build the answer. "
        base += "If a specific title is present, I'll match it approximately."
        return base


