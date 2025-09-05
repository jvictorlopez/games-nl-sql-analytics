# api/app/services/nlu.py
from dataclasses import dataclass
import re
import unicodedata
from typing import Optional, Tuple, List

def _strip_accents(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

@dataclass
class Intent:
    task: str                   # "top", "aggregate", "unknown"
    metric: str                 # "Global_Sales", "NA_Sales", "EU_Sales", "JP_Sales", "Other_Sales", "Critic_Score", "User_Score"
    top_n: int                  # default 10
    year: Optional[int] = None
    year_from: Optional[int] = None
    year_to: Optional[int] = None
    platform: Optional[str] = None
    genre: Optional[str] = None
    publisher: Optional[str] = None
    # aggregate fields
    agg: Optional[str] = None   # "avg", "sum", "count", etc.
    agg_field: Optional[str] = None
    name_like: Optional[str] = None  # e.g. "Zelda"

PORTUGUESE_TOP = re.compile(r"\btop\s*(\d+)\b")
YEAR = re.compile(r"\b(19[8-9]\d|20[0-2]\d)\b")
YEAR_RANGE = re.compile(r"\b(19[8-9]\d|20[0-2]\d)\D+(19[8-9]\d|20[0-2]\d)\b")

def parse_query(q: str) -> Intent:
    """
    Very small rule-based parser for our dataset fields.
    """
    raw = q.strip()
    txt = _strip_accents(raw.lower())

    # metric mapping
    metric = "Global_Sales"
    if any(k in txt for k in ["na", "america do norte", "eua"]):
        metric = "NA_Sales"
    elif any(k in txt for k in ["europa", "eu "]):  # 'eu ' to avoid matching 'eu' pronoun alone
        metric = "EU_Sales"
    elif "jap" in txt or "jp" in txt:
        metric = "JP_Sales"
    elif "outras" in txt or "other" in txt:
        metric = "Other_Sales"

    # top N
    m_top = PORTUGUESE_TOP.search(txt)
    top_n = int(m_top.group(1)) if m_top else 10

    # years
    yr_from = yr_to = year = None
    m_range = YEAR_RANGE.search(txt)
    if m_range:
        a, b = int(m_range.group(1)), int(m_range.group(2))
        yr_from, yr_to = min(a, b), max(a, b)
    else:
        m_year = YEAR.search(txt)
        if m_year:
            year = int(m_year.group(1))

    # aggregates
    # examples:
    # "media de nota da franquia zelda" -> avg User_Score on Name like '%Zelda%'
    # choose user_score if user mentions "usuarios", else critic_score if "critica"
    agg = None
    agg_field = None
    name_like = None
    if any(k in txt for k in ["media", "média", "avg", "average"]):
        agg = "avg"
        if "usuario" in txt or "usuarios" in txt or "user" in txt:
            agg_field = "User_Score"
        elif "crit" in txt or "metacritic" in txt:
            agg_field = "Critic_Score"
        else:
            # default if not specified
            agg_field = "User_Score"
        # look for franchise keyword after "franquia" or free token like zelda
        # simplest: pull a known token in quotes or capitalized word
        # here, if "zelda" in q, filter by Name like %Zelda%
        if "zelda" in txt:
            name_like = "Zelda"

    # Decide task
    task = "unknown"
    if any(k in txt for k in ["mais vendidos", "mais vendidas", "mais vendidos em", "top", "rank", "ranking", "vendas"]):
        task = "top"
    if agg is not None:
        task = "aggregate"

    return Intent(
        task=task,
        metric=metric,
        top_n=top_n,
        year=year,
        year_from=yr_from,
        year_to=yr_to,
        platform=None,
        genre=None,
        publisher=None,
        agg=agg,
        agg_field=agg_field,
        name_like=name_like
    )

def is_gaming_domain(q: str) -> bool:
    t = _strip_accents(q.lower())
    keywords = [
        "jogo", "jogos", "plataforma", "venda", "vendas", "genero", "gênero",
        "nintendo", "playstation", "xbox", "wii", "ds", "ps3", "ps4", "nes", "gb",
        "critica", "critico", "usuario", "metacritic", "score", "nota", "ano"
    ]
    return any(k in t for k in keywords)
