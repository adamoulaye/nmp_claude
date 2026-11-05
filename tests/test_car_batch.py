import sys

import pandas as pd

sys.path.insert(0, "tests")

from test_event_study import make_bars

from nmp.transform.car_batch import run_batch


def test_run_batch_results_and_errors():
    s, m = make_bars(shock=0.004)
    bars = pd.concat([s.assign(ticker="AAPL"), m.assign(ticker="SPY")], ignore_index=True)
    t = pd.Timestamp("2026-10-13 15:30:20", tz="UTC")
    news = pd.DataFrame(
        {"article_id": ["a1", "a2"], "ticker": ["AAPL", "TSLA"], "published_utc": [t, t]}
    )  # TSLA: aucune barre -> erreur
    res, win, err = run_batch(news, bars)
    assert list(res["article_id"]) == ["a1"] and res.loc[0, "p_0_15"] < 0.01
    assert set(win["article_id"]) == {"a1"} and len(win) == 76
    assert list(err["article_id"]) == ["a2"]
