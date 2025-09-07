from __future__ import annotations
import os, json, time
import requests

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL   = os.getenv("LOOKUP_MODEL", "gpt-4o-mini")

def chat_json(*, system: str, user: str, retries: int = 3) -> dict:
    assert OPENAI_API_KEY, "OPENAI_API_KEY não encontrado (carregue sua .env)"
    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0.0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }
    for i in range(retries):
        r = requests.post(
            OPENAI_API_URL,
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json=payload,
            timeout=60,
        )
        if r.status_code == 200:
            try:
                content = r.json()["choices"][0]["message"]["content"]
                return json.loads(content)
            except Exception as e:
                if i == retries - 1:
                    raise
        time.sleep(0.8 * (i + 1))
    raise RuntimeError("Falha ao obter JSON do modelo")

