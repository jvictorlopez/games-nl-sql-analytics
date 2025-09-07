from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

REQS = [
    ("Qual ano foi lançado o gta 5?", "lower(Name) = lower('Grand Theft Auto V')", "lookup_sql"),
    ("quantos jogos de ps4 existem no dataset?", "COUNT(*)", "lookup_sql"),
    ("em que ano saiu o wii sports?", "lower(Name) = lower('Wii Sports')", "lookup_sql"),
]


def _assert_reasoning_starts_pt(text: str):
    assert text.startswith("Vamos pensar passo a passo"), (
        f"Reasoning must start with required sentence. Got: {text[:80]}"
    )


def test_lookup_queries_work_and_show_sql():
    for q, must_contain, kind in REQS:
        r = client.get("/ask", params={"q": q})
        assert r.status_code == 200
        body = r.json()

        assert body["route"] == "sql", body
        assert body["kind"] == kind, body
        assert body["mode"] in ("dataset", "web"), body

        _assert_reasoning_starts_pt(body.get("reasoning", ""))

        sql = body.get("sql", "")
        assert sql and must_contain in sql, f"SQL missing or wrong:\n{sql}"

        nl = body.get("nl", "")
        assert nl and "Não consegui responder" not in nl, f"NL too generic: {nl}"

        assert "columns" in body and "rows" in body


