from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_top_2010():
    r = client.get("/ask", params={"q":"Quais são os jogos mais vendidos em 2010?"})
    assert r.status_code == 200
    js = r.json()
    assert js["route"] == "sql"
    assert "SELECT" in js["sql"]
    assert "Year_of_Release = 2010" in js["sql"]
    assert len(js["rows"]) > 0

def test_bounce():
    r = client.get("/ask", params={"q":"quanto custa uma banana?"})
    assert r.status_code == 200
    js = r.json()
    assert js["route"] == "bounce"
