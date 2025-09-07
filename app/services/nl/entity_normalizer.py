from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
import re
from functools import lru_cache
from rapidfuzz import process, fuzz
import pandas as pd

from app.services.datastore import get_datastore


ROMAN_MAP = {
    "i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5,
    "vi": 6, "vii": 7, "viii": 8, "ix": 9, "x": 10,
}

SERIES_SYNONYMS = {
    "gta": "grand theft auto",
    "cod": "call of duty",
    "mk": "mortal kombat",
    "smb": "super mario bros",
    "ff": "final fantasy",
    "nfs": "need for speed",
    "re4": "resident evil 4",
    "re": "resident evil",
    "rdr2": "red dead redemption 2",
    "rdr": "red dead redemption",
    "bof": "breath of fire",
}


def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    parts: List[str] = []
    for tok in s.split():
        if tok in SERIES_SYNONYMS:
            parts.extend(SERIES_SYNONYMS[tok].split())
            continue
        if tok in ROMAN_MAP:
            parts.append(str(ROMAN_MAP[tok]))
            continue
        parts.append(tok)
    return " ".join(parts)


@lru_cache(maxsize=1)
def _titles() -> List[str]:
    df = get_datastore().get_df()
    return df["Name"].dropna().astype(str).unique().tolist()


def resolve_title(q: str) -> Dict[str, Any]:
    cand = _norm(q)
    choices = _titles()
    res = process.extract(cand, [ _norm(x) for x in choices], scorer=fuzz.WRatio, limit=5)
    if not res:
        return {"canonical": None, "confidence": 0.0, "like": None, "suggestions": []}
    # Map back to original titles for display by matching normalized indices
    suggestions: List[Tuple[str, float]] = []
    for (norm_title, score, idx) in res:
        suggestions.append((choices[idx], float(score)))
    top_title, top_score = suggestions[0]
    like = f"%{_norm(top_title)}%".replace(" ", "%")
    return {"canonical": top_title if top_score >= 90 else None, "confidence": float(top_score), "like": like, "suggestions": suggestions}


def peek(q: str) -> Dict[str, Any]:
    # minimal signals for reasoning
    return {
        "year": _year(q := (q or "")),
        "region": _region(q),
    }


def _year(q: str) -> Optional[int]:
    m = re.search(r"\b(19\d{2}|20\d{2}|21\d{2})\b", q)
    return int(m.group(1)) if m else None


def _region(q: str) -> Optional[str]:
    ql = q.lower()
    if any(w in ql for w in ["jap", "japão", "japan", "jp"]):
        return "JP_Sales"
    if any(w in ql for w in ["europ", "europe", "eu"]):
        return "EU_Sales"
    if any(w in ql for w in ["américa do norte", "america do norte", "na", "eua", "us", "usa"]):
        return "NA_Sales"
    return "Global_Sales"


