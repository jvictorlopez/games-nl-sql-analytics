from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_top10_japan_metric():
    r = client.get("/ask", params={"q": "top 10 vendas no Japão"})
    assert r.status_code == 200
    j = r.json()
    assert j.get("route") in ("sql","bounce","not_found")
    if j.get("route") == "sql":
        assert j.get("chart",{}).get("type") == "bar"
        assert "JP_Sales" in (j.get("sql") or "") or j.get("meta",{}).get("metric_label") == "JP_Sales"


def test_franchise_avg_zelda_means():
    r = client.get("/ask", params={"q": "Qual a média de nota da franquia Zelda?"})
    assert r.status_code == 200
    j = r.json()
    assert j.get("route") in ("sql","not_found")


def test_rankings_2010_global():
    r = client.get("/ask", params={"q": "Quais são os jogos mais vendidos em 2010?"})
    assert r.status_code == 200
    j = r.json()
    assert j.get("route") == "sql"
    assert "2010" in (j.get("sql") or "")


def test_oos_banana():
    r = client.get("/ask", params={"q": "quanto custa uma banana"})
    assert r.status_code == 200
    j = r.json()
    assert j.get("route") == "bounce" or j.get("kind") == "out_of_domain"


