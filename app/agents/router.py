import re

DATASET_KWS = [
    "top", "mais vendidos", "vendas", "sales", "ranking", "rankings",
    "ano", "year", "plataforma", "platform", "gênero", "genero", "genre",
    "global", "na", "eua", "europa", "eu", "jp", "japão", "japao", "other",
    "crítica", "critica", "critic", "usuários", "usuarios", "user", "score",
]

OUT_OF_DOMAIN = [
    "stocks", "bitcoin", "clima", "weather", "trânsito", "traffic",
    "filme", "movie", "restaurante", "politics", "economy",
    "capital", "banana", "preço", "preco", "price"
]

def route_query(q: str) -> str:
    ql = q.lower()

    if any(kw in ql for kw in OUT_OF_DOMAIN):
        return "bounce"

    if any(kw in ql for kw in DATASET_KWS):
        return "dataset"

    # numbers often mean year / top N
    if re.search(r"\b(19[89]\d|20[0-2]\d)\b", ql) or re.search(r"\btop\s*\d+\b", ql):
        return "dataset"

    # fallback to dataset bias; web only if later proven missing & key enabled
    return "dataset"
def route_query(q: str) -> str:
    """
    Extremely lightweight router.
    Returns "dataset" unless the query clearly asks for news/real-time/external info.
    """
    ql = q.lower()
    external_markers = ["news", "notícia", "lançamento", "atual", "agora", "real time", "tempo real", "twitter", "wikipedia", "site"]
    if any(m in ql for m in external_markers):
        return "web"
    return "dataset"

