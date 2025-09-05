from typing import List
from rapidfuzz import process, fuzz
import pandas as pd
import re

TITLE_HINTS = ["jogo", "game", "título", "titulo", "chamado", "called", "o", "a"]

def _looks_like_title_query(q: str) -> bool:
    ql = q.lower()
    # heuristic: quotes or direct phrases that imply a specific title
    if '"' in q or "'" in q:
        return True
    if any(k in ql for k in ["qual é", "qual e", "informações sobre", "info sobre"]):
        return True
    # if it has 'jogo' and then a capitalized token sequence
    return bool(re.search(r"\bjog[oa]\b.*?[A-Z][A-Za-z0-9:\- ]{2,}", q))

def check_presence(q: str, df: pd.DataFrame, score_cutoff: int = 90) -> List[str]:
    if not _looks_like_title_query(q):
        return []
    names = df["Name"].astype(str).unique().tolist()
    results = process.extract(q, names, scorer=fuzz.WRatio, limit=5)
    hits = [n for n, s, _ in results if s >= score_cutoff]
    return hits

