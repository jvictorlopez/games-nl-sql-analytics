from app.core.config import get_settings

def web_fallback(q: str):
    # Stub: external calls disabled by default
    if not get_settings().ALLOW_WEB:
        return {"rows": []}
    # If you later enable, plug a local web cache or corporate search here
    return {"rows": []}

