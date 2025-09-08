# Games Analytics (FastAPI + Streamlit)

## Resumo Executivo

- FastAPI backend carrega `base_jogos.csv`, consolida e expõe analytics: panorama, rankings com filtros, busca, detalhes de jogo e estatísticas por franquia.
- DuckDB registra o CSV como tabela `games` para SQL in‑memory, com higienização de tipos e coerções; pandas é usado para ETL leve e respostas JSON.
- NL→SQL completo: orquestrador classifica intenção, `sql_agent` gera/valida/roda SQL, `nlg_agent` sumariza, e `lookup_sql_agent` resolve factóides (plataformas/ano/etc.) com execução em duas fases.
- Médias ponderadas por franquia (usuários e críticos) são computadas e retornadas em `meta.franchise_weighted`; o NL foi ajustado para sempre apresentar os números quando disponíveis.
- Streamlit UI (Dashboard + Chat) consome a API localmente (ou via docker), com métricas, rankings, gráficos e chat NL→SQL; spinner redundante removido.
- Testes automatizados cobrem endpoints e NL→SQL; `pytest -q` passou com 14 testes (em venv local).
- Diferenciais: fallback determinísticos (sem LLM), checagens de segurança SQL, override de NL para franquias, histograma das médias ponderadas, out‑of‑scope educado e mapeamento de região NA por linguagem natural.

---

## Matriz “Requisito do Teste vs Entrega”

| Requisito | Como foi atendido | Endpoint/Tela | Evidência |
|---|---|---|---|
| 1) Receber conjunto e torná‑lo consultável | `datastore.py` carrega CSV; DuckDB registra tabela `games` | Backend | Logs ao rodar: “DuckDB connection registered with table 'games'” |
| 2) Dados consolidados para estatísticas/rankings | ETL leve em pandas; SQL com casts/COALESCE; ranking agrega por título | `/dataset/summary`, `/games/rankings` | SUMMARY_KEYS: `['titles','years','global_sales_sum','critic_score_avg','user_score_avg','missing']` |
| 3a) Estatísticas descritivas | Endpoint retorna títulos, faixa de anos, soma de vendas e médias de notas | `/dataset/summary` | Ver SUMMARY_KEYS acima |
| 3b) Listas ordenadas (rankings) + filtros | `by=global|na|eu|jp|critic|user|combo` + filtros; NL→SQL também | `/games/rankings` e `/ask` | RANKINGS_NA_FIRST mostra `na_sales` corrigido; `/ask` para NA mapeia `metric_label=NA_Sales` |
| Busca por nome (string similarity) | RapidFuzz | `/games/search?q=zelda` | SEARCH_ZELDA_TOP3 retorna candidatos corretos |
| Detalhes de jogo | Seleção por nome e campos relevantes | `/games/{name}` | Retorna colunas clássicas (ano, críticas, usuários) |
| Estatística por franquia | Cálculo ponderado e histograma | `/franchises/{slug}` e `/ask` | `meta.franchise_weighted` e NL override presente |
| Extras | NL→SQL; fallback determinísticos; UI Streamlit | Streamlit + agentes | Evidências NL→SQL abaixo |

---

## Arquitetura & Fluxo

- Fluxo NL→SQL (texto):
  1. Orchestrator (`orchestrator.route_and_execute`) recebe `q`, detecta intenção com heurísticas (ano, região, franquia, ranking).
  2. Se intenção for ranking/summary/franchise, passa um PLANO a `sql_agent.llm_build_sql_and_run` que (a) tenta LLM, (b) valida e (c) executa no DuckDB; computa `meta` (incl. `franchise_weighted`).
  3. `nlg_agent.summarize_in_domain` gera resposta NL; para `franchise_avg`, o orquestrador aplica um override determinístico garantindo texto PT‑BR com médias ponderadas.
  4. Sobras fora dos 3 escopos: lookup lane em duas fases via `lookup_sql_agent.call_lookup_agent` (gera SQL, executa, pede NL final).
- DuckDB: `get_datastore()` carrega CSV, registra `games`; `sql_agent` usa `ensure_table_registered` e SQL com `try_cast`, `COALESCE`.

---

## Endpoints

| Endpoint | Propósito | Parâmetros principais | Exemplo | Campos‑chave (amostra truncada) |
|---|---|---|---|---|
| GET `/health` | Smoke | — | `/health` | `{"status":"ok","version":"1.0.0"}` |
| GET `/dataset/summary` | Panorama do dataset | — | `/dataset/summary` | `{"titles":16719,"years":{"min":1980,"max":2020},"global_sales_sum":8920.3,"critic_score_avg":69.0,"user_score_avg":71.3,...}` |
| GET `/games/rankings` | Rankings | `by, n, platform, genre, year, year_from, year_to, publisher, developer, franchise` | `/games/rankings?by=global&n=5` | item exemplo: `{"name":"Wii Sports","year":2006,"global_sales":82.53,"na_sales":41.36,...}` |
| GET `/games/search` | Busca fuzzy | `q, limit` | `/games/search?q=zelda&limit=3` | `[{"name":"The Legend of Zelda","platform":"NES","year":1986,"score":80.0}, ...]` |
| GET `/games/{name}` | Detalhes | `platform, year` | `/games/Wii%20Sports` | linhas com críticas/usuários por plataforma/ano |
| GET `/franchises/{slug}` | Estatística por franquia | slug | `/franchises/zelda` | `{"total_titles":..., "avg_critic":..., "avg_user":..., "top_entries":[...]}` |
| GET `/ask` | NL→SQL unificada | `q` | `/ask?q=top 7 vendas na américa do norte` | `{"kind":"rankings","meta":{"metric_label":"NA_Sales",...}}` |

---

## Demonstrações NL→SQL (JSON real)

- Franquia (médias ponderadas): “Qual a média de nota da franquia Zelda?”
  - kind: franchise_avg
  - NL (override): 
    - “Média ponderada da franquia: Críticos 88.9 (n=857), Usuários 86.1 (n=8527), 15 títulos considerados.”
  - meta.franchise_weighted (truncado):
    ```json
    {
      "critic_wavg": 88.9066,
      "user_wavg": 86.0502,
      "combo_wavg": 87.7641,
      "critic_count_sum": 857,
      "user_count_sum": 8527,
      "total_titles": 15
    }
    ```
- Ranking regional: “top 7 vendas na américa do norte”
  - kind: rankings
  - meta: `{"metric_label":"NA_Sales","intent":"rankings"}`
- Lookup factual: “em quais plataformas saiu the witcher 3?”
  - NL: “As plataformas em que 'The Witcher 3: Wild Hunt' saiu são: XOne, PS4, PC.”
- Fora de escopo: “top 10 bananas”
  - NL: “Sua pergunta parece estar fora do escopo deste app (focado em dados de videogames). Tente: 'Top 10 vendas globais em 2010', 'Top 10 no Japão por User_Score', 'Média de nota da franquia Zelda'.”

Observações:
- Para “Prison Break”, o agente retorna “Não consegui responder com lookup.” (dataset coverage inexistente), condizente com screenshot (“Não consegui responder…”).

---

## Frontend (Streamlit)

- Abas:
  - Dashboard: métricas de panorama, filtros de ranking (ordenar por global/regionais/avaliações, N, plataforma, gênero, anos), tabela e gráficos (barras e scatter). Ver “Gráficos” nas imagens.
  - Chat (NL→SQL): input único, mostra pergunta, resumo (bolha NL), expander de raciocínio do agente, expander de SQL executado e tabela/visualizações quando aplicável. Spinner “Interpretando intenção…” foi removido (sem delay artificial); indicador global de execução preservado.
- Consumo da API: `API_URL` (ou `http://api:8000` / `http://localhost:8000`).
- Referências às screenshots:
  - Screenshot 1: “Não consegui responder com lookup.” para `prison break`.
  - Screenshot 2: Fora de escopo “top 10 bananas”.
  - Screenshot 3: Lookup de plataformas “the witcher 3”.
  - Screenshot 4: Ranking NA com `NA_Sales` e gráfico.
  - Demais: UI geral do dashboard e schemas dos modelos via docs.

---

## Testes

- Execução local (venv):
  - `pytest -q` → “14 passed, 3 warnings”
- Cobertura principal:
  - Endpoints smoke e NL→SQL e2e.
  - Regressão lookup (garante `data.result` presente para 1x1).
  - Intenção/região NA (“américa do norte” → `metric_label='NA_Sales'`).
- Gaps/sugestões:
  - Adicionar casos multi‑filtros (publisher + platform + ranges).
  - Casos de detalhes com platform/year específicos.
  - Casos lookup adicionais (publisher/genre/developer listagem).

---

## Limitações & Próximos Passos

- Dependência opcional de LLM: há fallbacks determinísticos, porém melhorias de extração de entidades (plataforma, franqui­a) podem migrar para `entity_normalizer.resolve_title`.
- Segurança/observabilidade: já existem validações e logs; próximos passos incluem tracing distribuído e métricas Prometheus.
- UI: poderia oferecer edição do SQL com execução direta e salvar consultas favoritas.
- Autenticação/autorização: não implementadas (contexto on‑prem interno).

---

## Como Rodar

- Inserir chave OpenAI:
  Criar .env e inserir OPENAI_API_KEY="CHAVE"

- Local (venv):
  ```bash
  python3 -m venv .venv
  . .venv/bin/activate
  pip install -r requirements.txt
  uvicorn app.main:app --host 0.0.0.0 --port 8000
  streamlit run streamlit_app/app.py  # em outro terminal
  pytest -q
  ```
- Docker:
  ```bash
  docker compose up --build
  # API: http://localhost:8000/docs
  # UI:  http://localhost:8501
  ```
- Variáveis:
  - `API_URL` para a UI (se necessário); CSV já apontado para `base_jogos.csv` na raiz (Docker copia para `/app/data` também).

---

## Diferenciais & Decisões de Design

- Separação clara de agentes: orquestrador (fluxo), `sql_agent` (SQL + segurança + pós‑processamento meta), `nlg_agent` (NL concisa), `lookup_sql_agent` (duas fases).
- Médias ponderadas por franquia + histograma; override NL determinístico p/ garantir clareza em PT‑BR.
- NL→SQL robusto com fallback deterministicamente correto; mapeamento regional NA por linguagem natural (incl. “américa do norte”).
- UX Streamlit com chat‑bubble NL, expander de raciocínio e SQL, gráficos dinâmicos; remoção de spinner redundante.
