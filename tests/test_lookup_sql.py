from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_year_lookup_gta5():
    r = client.get("/ask", params={"q": "when did gta 5 come out"})
    assert r.status_code == 200
    body = r.json()
    assert body["mode"] in ("dataset","web")
    assert body.get("route") == "sql"
    assert body.get("kind") == "lookup_sql"
    data = body.get("data", {})
    assert "result" in data
    assert int(data["result"]) == 2013
    assert "2013" in (body.get("nl") or "")


