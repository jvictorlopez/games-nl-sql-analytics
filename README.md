# Games Analytics (FastAPI + Streamlit)

Serviço interno, on-prem, para responder curiosidades sobre vendas e avaliações de videogames a partir de `base_jogos.csv`.

## Rodando com Docker
```bash
docker compose up --build
# API:     http://localhost:8000/docs
# Streamlit UI: http://localhost:8501
```

## Rodando local (dev)
```bash
python -m venv .venv && source .venv/bin/activate  # (Linux/macOS) ou .venv\Scripts\activate (Windows)
pip install -r requirements.txt
export GAMES_CSV_PATH=base_jogos.csv
uvicorn app.main:app --reload
# Em outro terminal:
streamlit run streamlit_app/app.py
```

## Endpoints
GET /health

GET /dataset/summary

GET /games/rankings?by=global|na|eu|jp|other|critic|user|combo&n=10&platform=&genre=&year=&year_from=&year_to=&publisher=&developer=&franchise=

GET /games/search?q=...

GET /games/{name}?platform=&year=

GET /franchises/{slug}

## Notas
User_Score normalizado (×10) para 0–100.

Score combinado (by=combo): 0.6Critic + 0.4User, com ponderação opcional por contagens.

Heurística de franquias em app/utils/franchises.json.

Caching LRU e logs de tempo de execução.

## Testes
```bash
pytest -q
```

