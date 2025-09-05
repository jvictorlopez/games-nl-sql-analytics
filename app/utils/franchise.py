import json
from pathlib import Path
from typing import Optional, List

_MAP = None

def _load_map():
    global _MAP
    if _MAP is None:
        path = Path(__file__).resolve().parent / "franchises.json"
        _MAP = json.loads(path.read_text(encoding="utf-8"))
    return _MAP

def infer_franchise(name: str) -> Optional[str]:
    m = _load_map()
    for slug, patterns in m.items():
        for p in patterns:
            if p.lower() in name.lower():
                return slug
    return None

