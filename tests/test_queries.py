import os
import requests

API = os.environ.get("API_URL", "http://localhost:8000")

QUERIES = [
    "Quais são os jogos mais vendidos em 2010?",
    "Qual a média de nota da franquia Zelda?",
    "Top 5 jogos da Nintendo por vendas globais",
]

for q in QUERIES:
    print("\n=== Q:", q)
    r = requests.get(f"{API}/ask", params={"q": q}, timeout=60)
    print("status:", r.status_code)
    try:
        data = r.json()
    except Exception as e:
        print("non-json:", r.text)
        continue
    print("SQL:", data.get("sql"))
    rows = data.get("rows") or []
    cols = data.get("columns") or []
    print("rows:", len(rows))
    if rows and cols:
        # show first row
        print("first:", dict(zip(cols, rows[0])))
