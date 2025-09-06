import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.agents import orchestrator


client = TestClient(app)


@pytest.fixture(autouse=True)
def no_llm(monkeypatch):
    def fake_reason_and_plan(q: str):
        ql = q.lower()
        if "jap" in ql:
            return ("Ranking por JP_Sales.", {
                "intent":"rankings","metric":"jp","topn":10,
                "filters":{"year":None,"platform":None,"genre":None,"publisher":None,"developer":None,"franchise":None,"name":None}
            })
        if "2010" in ql:
            return ("Ranking por Global_Sales em 2010, agregando títulos.", {
                "intent":"rankings","metric":"global","topn":10,
                "filters":{"year":2010,"platform":None,"genre":None,"publisher":None,"developer":None,"franchise":None,"name":None}
            })
        if "média" in ql and "zelda" in ql:
            return ("Média ponderada da franquia Zelda.", {
                "intent":"franchise_avg","metric":"user","topn":None,
                "filters":{"year":None,"platform":None,"genre":None,"publisher":None,"developer":None,"franchise":"zelda","name":None}
            })
        if "breath of the wild" in ql:
            return ("Verificar se título existe. Não encontrado.", {
                "intent":"not_found","metric":None,"topn":None,
                "filters":{"year":None,"platform":None,"genre":None,"publisher":None,"developer":None,"franchise":None,"name":"zelda breath of the wild"}
            })
        if "banana" in ql:
            return ("Fora do escopo.", {
                "intent":"oob","metric":None,"topn":None,
                "filters":{"year":None,"platform":None,"genre":None,"publisher":None,"developer":None,"franchise":None,"name":None}
            })
        return ("Sem plano claro.", {
            "intent":"not_found","metric":None,"topn":None,
            "filters":{"year":None,"platform":None,"genre":None,"publisher":None,"developer":None,"franchise":None,"name":None}
        })
    monkeypatch.setattr(orchestrator, "reason_and_plan", fake_reason_and_plan)


def test_japan_ranking_metric():
    r = client.get("/ask", params={"q":"top 10 vendas no Japão"})
    data = r.json()
    assert r.status_code == 200
    assert data["kind"] == "rankings"
    assert data["meta"]["metric_label"].lower().startswith("jp_") or "JP_Sales" in data["columns"]


def test_year_filter_2010_in_sql():
    r = client.get("/ask", params={"q":"Quais são os jogos mais vendidos em 2010?"})
    data = r.json()
    assert "2010" in (data.get("sql") or "")
    s = (data.get("sql") or "").lower()
    assert ("group by" in s) or ("grouped" in s)


def test_franchise_avg_zelda():
    r = client.get("/ask", params={"q":"Qual a média de nota da franquia Zelda?"})
    data = r.json()
    assert data["kind"] == "franchise_avg"
    assert data["meta"]["metric_label"] in ["User_Score","Critic_Score","score_combo"]


def test_not_found_title():
    r = client.get("/ask", params={"q":"qual o ano de lançamento de zelda breath of the wild?"})
    data = r.json()
    assert data["kind"] == "not_found"
    assert "Não encontrei" in data["nl"]


def test_out_of_domain():
    r = client.get("/ask", params={"q":"qual o preço médio da banana no brasil?"})
    data = r.json()
    assert data["kind"] == "out_of_domain"


