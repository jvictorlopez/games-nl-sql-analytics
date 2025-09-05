import os
import re
import requests
import streamlit as st
import pandas as pd
import plotly.express as px

CANDIDATES = [
    os.getenv("API_URL", ""),
    "http://api:8000",
    "http://localhost:8000",
]

@st.cache_data(show_spinner=False)
def pick_api() -> str:
    for base in CANDIDATES:
        if not base:
            continue
        try:
            r = requests.get(f"{base}/health", timeout=2)
            if r.ok:
                return base
        except Exception:
            continue
    return ""

API = pick_api()

st.set_page_config(page_title="Games Analytics (Local)", layout="wide")
st.title("🎮 Games Analytics (Local)")

if not API:
    st.error("Não consegui achar a API (tentativas: api:8000 e localhost:8000). Suba `docker compose up` ou defina API_URL.")
    st.stop()

tabs = st.tabs(["📊 Dashboard", "💬 Chat (NL → SQL)"])

# ------------------ DASHBOARD ------------------
with tabs[0]:
    st.subheader("Panorama do Dataset")
    try:
        summary = requests.get(f"{API}/dataset/summary", timeout=10).json()
    except Exception as e:
        st.exception(e)
        st.stop()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Títulos", summary["titles"])
    c2.metric("Ano Mín.", summary["years"]["min"])
    c3.metric("Ano Máx.", summary["years"]["max"])
    c4.metric("Vendas Globais (M)", f'{summary["global_sales_sum"]:.2f}')
    c5.metric("Média Crítica / Usuários", f'{(summary["critic_score_avg"] or 0):.1f} / {(summary["user_score_avg"] or 0):.1f}')

    st.divider()
    with st.expander("Filtros"):
        colf1, colf2, colf3, colf4, colf5 = st.columns(5)
        by = colf1.selectbox("Ordenar por", ["global","na","eu","jp","other","critic","user","combo"], index=0)
        n = colf2.number_input("Top N", 5, 100, 10)
        platform = colf3.text_input("Platform")
        genre = colf4.text_input("Genre")
        year = colf5.number_input("Year (exato)", 0, 9999, 0)
        colf6, colf7 = st.columns(2)
        year_from = colf6.number_input("Year From", 0, 9999, 0)
        year_to = colf7.number_input("Year To", 0, 9999, 0)

    params = dict(by=by, n=n)
    if platform: params["platform"] = platform
    if genre: params["genre"] = genre
    if year: params["year"] = int(year)
    if year_from: params["year_from"] = int(year_from)
    if year_to: params["year_to"] = int(year_to)

    res = requests.get(f"{API}/games/rankings", params=params, timeout=20).json()
    df = pd.DataFrame(res["items"])
    st.subheader("Rankings")
    st.dataframe(df)

    st.subheader("Gráficos")
    if not df.empty:
        c1, c2 = st.columns(2)
        with c1:
            if "global_sales" in df:
                fig = px.bar(df, x="name", y="global_sales", title="Top por Vendas Globais")
                st.plotly_chart(fig, use_container_width=True)
        with c2:
            if {"critic_score","user_score"}.issubset(df.columns):
                size_series = df.get("user_count", pd.Series([0]*len(df))).fillna(0).clip(lower=1)
                fig2 = px.scatter(
                    df, x="critic_score", y="user_score", size=size_series,
                    hover_name="name", title="Crítica vs Usuários"
                )
                st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    st.subheader("Busca de Jogos")
    q = st.text_input("Nome do jogo")
    if q:
        hits = requests.get(f"{API}/games/search", params={"q": q}).json()["hits"]
        st.json(hits)
        if hits:
            choice = hits[0]
            det = requests.get(f"{API}/games/{choice['name']}", params={"platform": choice.get("platform"), "year": choice.get("year")}).json()
            st.json(det)

# --- NL -> SQL Chat ----------------------------------------------------------
with tabs[1]:
    import time
    from urllib.parse import quote

    # State machine for prompt -> pending -> result
    if "agent" not in st.session_state:
        st.session_state.agent = {
            "pending": False,
            "prompt": None,
            "result": None,
        }

    st.header("Chat (NL → SQL)")

    # Single input, non-empty label (hidden), no warnings
    with st.form("nl_sql_form", clear_on_submit=True):
        prompt = st.text_input(
            label="Pergunte em linguagem natural",
            value="",
            placeholder="Ex.: top 10 por vendas globais em 2010",
            label_visibility="collapsed",
            key="nlq_input",
        )
        submitted = st.form_submit_button("→", use_container_width=False)

    # Submit: stash prompt, mark pending, and rerun immediately
    if submitted and prompt.strip():
        st.session_state.agent["prompt"] = prompt.strip()
        st.session_state.agent["result"] = None
        st.session_state.agent["pending"] = True
        st.rerun()

    # Worker: on the rerun after submit, do the "thinking", call API, store result, rerun
    def render_thinking(total_sec: float = 3.0):
        # Simulate lighting/shimmer with status steps (x/3 each)
        step = max(0.1, total_sec / 3.0)
        with st.status("🤖 Processando qual agente chamar…", state="running") as status:
            time.sleep(step)
            status.update(label="🤖 Interpretando intenção do usuário…", state="running")
            time.sleep(step)
            status.update(label="🤖 Preparando resposta…", state="running")
            time.sleep(step)
            status.update(label="Pronto!", state="complete")

    def call_api(nlq: str) -> dict:
        try:
            r = requests.get(f"{API}/ask?q={quote(nlq)}", timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            return {"route": "error", "reason": f"Erro ao consultar API: {e}"}

    if st.session_state.agent["pending"] and st.session_state.agent["prompt"]:
        # show UX thinking
        render_thinking(3.0)
        # do the actual call
        res = call_api(st.session_state.agent["prompt"])
        # commit result
        st.session_state.agent["result"] = res
        st.session_state.agent["pending"] = False
        st.rerun()

    # Renderer: single, non-stacking result area
    # --- [ADD] util: histograma de médias ponderadas para franquias ---
    def _render_franchise_histogram(payload):
        import math
        import plotly.graph_objects as go
        import pandas as pd
        from streamlit import caption, plotly_chart, markdown

        fw = (payload.get("meta") or {}).get("franchise_weighted") or {}
        u  = fw.get("user_wavg")
        c  = fw.get("critic_wavg")
        uc = (fw.get("user_count_sum") or 0)
        cc = (fw.get("critic_count_sum") or 0)

        # calcula "Geral (wavg)" com pesos das contagens; fallback: média simples dos disponíveis
        overall = None
        nums, weights = [], []
        if u is not None and uc > 0:
            nums.append(float(u)); weights.append(float(uc))
        if c is not None and cc > 0:
            nums.append(float(c)); weights.append(float(cc))
        if weights:
            overall = sum(n*w for n, w in zip(nums, weights)) / sum(weights)
        elif nums:
            overall = sum(nums) / len(nums)

        # prepara dados (só inclui métricas que existem)
        bars = []
        if u is not None: bars.append(("Usuários (wavg)", float(u)))
        if c is not None: bars.append(("Crítica (wavg)", float(c)))
        if overall is not None: bars.append(("Geral (wavg)", float(overall)))
        if not bars:
            return  # nada para plotar

        x = [b[0] for b in bars]
        y = [round(b[1], 2) for b in bars]

        fig = go.Figure(go.Bar(
            x=x, y=y,
            marker=dict(color="white"),     # barras brancas
            hovertemplate="%{x}: %{y:.2f}<extra></extra>",
        ))
        fig.update_layout(
            template="plotly_dark",
            height=280,
            margin=dict(l=16, r=16, t=16, b=16),
            yaxis=dict(title="Média ponderada", rangemode="tozero"),
            xaxis=dict(title=""),
        )

        markdown("#### Médias ponderadas (histograma)")
        plotly_chart(fig, use_container_width=True)
        caption(f"Base: usuários n={uc}, críticos n={cc}")

    def render_result(res: dict, nlq_used: str):
        st.subheader("🧠 Pergunta:")
        st.write(nlq_used)

        st.subheader("Resumo:")
        # --- Agent NL answer (chat bubble) ---
        nl_summary = res.get("nl") or res.get("summary") or res.get("notice") or res.get("message") or ""
        if nl_summary:
            st.markdown(
                """
                <div style="background-color:#0f2033;border-radius:10px;padding:14px 16px;margin:8px 0;display:flex;gap:10px;align-items:flex-start;">
                  <div style="flex:0 0 22px;font-size:18px;line-height:22px;">🤖</div>
                  <div style="color:#d9e2ec;font-size:15px;">{}</div>
                </div>
                """.format(nl_summary),
                unsafe_allow_html=True
            )

        # SQL expander under bubble when present
        if res.get("sql"):
            with st.expander("SQL executado", expanded=False):
                st.code(res["sql"], language="sql")

        cols = res.get("columns") or []
        rows = res.get("rows") or []
        if cols and rows:
            df = pd.DataFrame(rows, columns=cols)

            # Minimal table for rankings
            if res.get("kind") == "rankings":
                metric_candidates = [c for c in df.columns if c not in ("Rank", "Name", "year")]
                if metric_candidates:
                    metric = metric_candidates[-1]
                    df = df[["Rank", "Name", "year", metric]]
            st.dataframe(df, use_container_width=True, hide_index=True)

            # --- [CALL] histograma de franquia quando aplicável ---
            if res.get("kind") == "franchise_avg":
                _render_franchise_histogram(res)

            # Charts: for rankings, plot Name vs selected metric
            if res.get("kind") == "rankings":
                metric_candidates = [c for c in df.columns if c not in ("Rank", "Name", "year")]
                if metric_candidates and "Name" in df.columns:
                    ycol = metric_candidates[-1]
                    df_sorted = df.sort_values(ycol, ascending=False)
                    fig = px.bar(
                        df_sorted,
                        x="Name",
                        y=ycol,
                        title=f"Top por {ycol.replace('_', ' ').title()}",
                        color_discrete_sequence=px.colors.sequential.YlOrBr,
                    )
                    fig.update_layout(
                        xaxis_title="Nome",
                        yaxis_title=ycol.replace('_', ' ').title(),
                        margin=dict(l=10, r=10, t=60, b=10),
                    )
                    fig.update_xaxes(tickangle=-30)
                    st.plotly_chart(fig, use_container_width=True)

    # Show result if we have one; otherwise show nothing (no "Sem resultados" noise)
    if st.session_state.agent["result"]:
        render_result(st.session_state.agent["result"], st.session_state.agent["prompt"])