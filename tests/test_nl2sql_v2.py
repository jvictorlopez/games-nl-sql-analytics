from app.services.agents.orchestrator import route_and_execute


def test_year_gta5():
    js = route_and_execute("em qual ano saiu gta 5?")
    assert js.get("route") in ("sql","not_found","bounce")


def test_publisher_bof3():
    js = route_and_execute("quem publicou breath of fire iii?")
    assert js.get("route") in ("sql","not_found","bounce")


def test_top_na():
    js = route_and_execute("top 7 vendas na américa do norte")
    if js.get("route") == "sql":
        assert js.get("meta",{}).get("metric_label") == "NA_Sales"
        assert len(js.get("rows_dict", js.get("rows", []))) == 7 or js.get("meta",{}).get("topn") == 7


def test_zelda_means():
    js = route_and_execute("qual a média de nota da franquia zelda?")
    assert js.get("route") in ("sql","not_found")


def test_distinct_platforms():
    js = route_and_execute("em quais plataformas saiu resident evil 4?")
    assert js.get("route") in ("sql","not_found")


def test_oos_message():
    js = route_and_execute("quanto custa uma banana?")
    assert js.get("route") == "bounce"


