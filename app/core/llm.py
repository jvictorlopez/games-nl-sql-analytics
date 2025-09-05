import os
from typing import Optional, List, Dict, Any

# Legacy openai SDK path (widely available)
try:
    import openai  # type: ignore
except Exception:  # pragma: no cover
    openai = None  # type: ignore

MODEL_DEFAULT = os.getenv("OPENAI_MODEL", "gpt-5-high")
API_KEY = os.getenv("OPENAI_API_KEY")


def chat(messages: List[Dict[str, Any]], temperature: float = 0.2, model: Optional[str] = None) -> Optional[str]:
    """
    Return assistant content or None when the LLM is not available.
    """
    if openai is None or not API_KEY:
        return None
    openai.api_key = API_KEY  # type: ignore
    mdl = model or MODEL_DEFAULT
    try:
        resp = openai.ChatCompletion.create(model=mdl, messages=messages, temperature=temperature)  # type: ignore
        return resp["choices"][0]["message"]["content"]
    except Exception:
        return None


def get_model() -> str:
    return MODEL_DEFAULT


