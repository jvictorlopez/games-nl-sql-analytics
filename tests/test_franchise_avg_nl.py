import os
import requests

BASE = os.getenv("API_BASE", "http://localhost:8000")


def test_franchise_avg_nl_ptbr():
    r = requests.get(f"{BASE}/ask", params={"q": "Qual a média de nota da franquia Zelda?"}, timeout=60)
    r.raise_for_status()
    data = r.json()
    assert data["kind"] == "franchise_avg"
    nl = data.get("nl") or ""
    assert "não disponíveis" not in nl.lower()
    assert ("Média ponderada" in nl) or ("Críticos" in nl and "Usuários" in nl)
    assert any(ch in nl for ch in [",", "."])  # likely decimal formatting


