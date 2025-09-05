from app.services.datastore import get_datastore
from app.agents.router import route_query
from app.agents.presence import check_presence
from app.agents.sqlgen import nl_to_sql_plan, sql_with_plot, run_duckdb_sql


def _df():
    return get_datastore().get_df()


def test_route_generic_goes_dataset():
    q = "Quais são os jogos mais vendidos em 2010?"
    assert route_query(q) == "dataset"


def test_presence_not_called_for_generic():
    q = "Top 5 por vendas globais em 2008"
    assert check_presence(q, _df()) == []


def test_nl_plan_year_metric_top():
    q = "Top 5 por vendas no Japão em 2009 no DS"
    plan = nl_to_sql_plan(q, _df())
    assert plan["topn"] == 5
    assert plan.get("year_exact") == 2009


def test_sql_exec_works():
    q = "Top 3 vendas globais entre 2005 e 2010"
    sql, _ = sql_with_plot(q, _df())
    out = run_duckdb_sql(sql, "games", _df())
    assert len(out) <= 3
    assert not out.empty

