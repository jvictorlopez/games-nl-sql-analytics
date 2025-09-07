#!/usr/bin/env python3
import os, sys, time, json, requests
from typing import Dict, Any, List

# Carrega .env automaticamente
try:
    from dotenv import load_dotenv
    load_dotenv(".env")
except Exception:
    pass

API = os.getenv("LOOKUP_API", "http://localhost:8000/ask")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EVAL_MODEL = os.getenv("LOOKUP_EVAL_MODEL", "gpt-5.1-mini")

if not OPENAI_API_KEY:
    print("ERRO: OPENAI_API_KEY não está no ambiente (.env).", file=sys.stderr)
    sys.exit(1)

TESTS = [
    {
        "q": "Qual ano foi lançado o gta 5?",
        "must_sql": ["lower(Name) = lower('Grand Theft Auto V')", "MIN(Year_of_Release)"],
        "desc": "Year GTA V",
    },
    {
        "q": "quantos jogos de ps4 existem no dataset?",
        "must_sql": ["COUNT(", "lower(Platform) = lower('PS4')"],
        "desc": "Count PS4",
    },
    {
        "q": "em que ano saiu o wii sports?",
        "must_sql": ["lower(Name) = lower('Wii Sports')", "MIN(Year_of_Release)"],
        "desc": "Year Wii Sports",
    },
]

JUDGE_SYS = """You are a strict evaluator. You receive API JSON with keys: question, reasoning, sql, columns, rows, nl.
Answer two booleans:
1) sql_ok: true/false — Does `sql` correctly retrieve what `question` asks (single safe SELECT, correct columns/filters, exact equality for titles)?
2) nl_ok: true/false — Does `nl` faithfully reflect `rows`/`columns` (no hallucination)?
Return STRICT JSON: {"sql_ok": true|false, "nl_ok": true|false, "notes": "<brief>"}"""

def judge(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = "https://api.openai.com/v1/chat/completions"
    data = {
        "model": EVAL_MODEL,
        "response_format": {"type": "json_object"},
        "temperature": 0.0,
        "messages": [
            {"role": "system", "content": JUDGE_SYS},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}
        ],
    }
    r = requests.post(url, headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}, json=data, timeout=60)
    r.raise_for_status()
    out = r.json()["choices"][0]["message"]["content"]
    return json.loads(out)

def run_once() -> List[Dict[str, Any]]:
    outs = []
    for t in TESTS:
        r = requests.get(API, params={"q": t["q"]}, timeout=60)
        body = r.json()
        sql = body.get("sql","")
        nl = body.get("nl","")
        reasoning = body.get("reasoning","")
        cols = body.get("columns", [])
        rows = body.get("rows", [])
        hard = all(s in sql for s in t["must_sql"]) and body.get("route") == "sql" and body.get("kind") == "lookup_sql"
        prefix_ok = reasoning.startswith("Vamos pensar passo a passo") or reasoning.startswith("Let’s think step by step")
        jd = judge({"question": t["q"], "reasoning": reasoning, "sql": sql, "columns": cols, "rows": rows, "nl": nl})
        outs.append({"desc": t["desc"], "http": r.status_code, "sql": sql, "nl": nl, "hard": hard, "prefix_ok": prefix_ok, "judge": jd, "body": body})
    return outs

def main():
    tries = int(os.getenv("LOOKUP_MAX_TRIES", "3"))
    for i in range(1, tries+1):
        print(f"\n=== Attempt {i} ===")
        all_ok = True
        res = run_once()
        for o in res:
            sql_ok = bool(o["judge"].get("sql_ok"))
            nl_ok  = bool(o["judge"].get("nl_ok"))
            ok = o["http"] == 200 and o["hard"] and o["prefix_ok"] and sql_ok and nl_ok
            all_ok &= ok
            status = "PASS" if ok else "FAIL"
            print(f"[{status}] {o['desc']}")
            if not ok:
                print("  route/kind:", o["body"].get("route"), o["body"].get("kind"))
                print("  SQL:", o["sql"]) 
                print("  NL :", o["nl"]) 
                print("  Reasoning prefix ok?", o["prefix_ok"]) 
                print("  Judge:", o["judge"]) 
        if all_ok:
            print("\n✅ Lookup LLM-eval PASS.")
            return 0
        time.sleep(1.5)
    print("\n❌ Lookup LLM-eval FAIL.")
    return 1

if __name__ == "__main__":
    sys.exit(main())


