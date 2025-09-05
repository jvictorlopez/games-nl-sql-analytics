import os
from fastapi.testclient import TestClient
from app.main import app

def test_health():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

def test_summary():
    client = TestClient(app)
    r = client.get("/dataset/summary")
    assert r.status_code == 200
    body = r.json()
    assert "titles" in body

def test_rankings_basic():
    client = TestClient(app)
    r = client.get("/games/rankings?by=global&n=5")
    assert r.status_code == 200
    assert len(r.json()["items"]) <= 5

