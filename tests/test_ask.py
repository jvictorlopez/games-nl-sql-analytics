from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_ask_basic():
    r = client.get("/ask", params={"q": "top 3 by global sales"})
    assert r.status_code == 200
    body = r.json()
    assert body["mode"] in ("dataset","web")
    assert "rows" in body

