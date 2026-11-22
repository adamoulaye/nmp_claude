"""Test de bout en bout du mode local : archives Bronze synthetiques -> evenements -> CAR.

Verifie les proprietes defendues dans le rapport : rejouer une archive ne cree pas de doublon,
les reprises et les nouvelles hors seance ne sont pas des evenements, un choc APRES la publication
est detecte, et le resultat est deterministe. Aucun reseau, aucun acces a data/ ni a Databricks.
"""

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_event_study import make_bars  # noqa: E402

from nmp.local.events_batch import run, summarize  # noqa: E402

TICKERS = ["AAPL", "TSLA", "SPY"]


def _write(data_dir, source, key, day, batch, payload):
    d = Path(data_dir) / "bronze" / source / key / day
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{batch}.json").write_text(json.dumps({"key": key, "payload": payload}), encoding="utf-8")


def _bars_page(stock, market):
    def rows(df):
        return [
            {"t": t.isoformat(), "o": c, "h": c, "l": c, "c": c, "v": int(v), "n": 1, "vw": c}
            for t, c, v in zip(df["ts_utc"], df["close"], df["volume"])
        ]

    return {"bars": {"AAPL": rows(stock), "SPY": rows(market)}}


def _article(i, created, symbol, headline):
    return {
        "id": i,
        "created_at": created,
        "updated_at": created,
        "source": "benzinga",
        "symbols": [symbol],
        "headline": headline,
        "summary": "",
        "url": f"http://exemple.test/{i}",
    }


@pytest.fixture()
def data_dir(tmp_path):
    stock, market = make_bars(shock=0.004)
    bars = _bars_page(stock, market)
    _write(tmp_path, "alpaca_bars", "lot", "2026-10-13", "001", [bars])
    _write(tmp_path, "alpaca_bars", "lot", "2026-10-13", "002", [bars])  # reprise : meme lot telecharge deux fois
    news = {
        "news": [
            _article(1, "2026-10-13T15:30:20Z", "AAPL", "Apple announces record iPhone sales"),
            _article(2, "2026-10-13T15:45:00Z", "AAPL", "Apple announces record iPhone sales"),  # reprise du titre
            _article(3, "2026-10-13T23:00:00Z", "AAPL", "Apple after hours note"),  # apres la cloture
            _article(4, "2026-10-13T15:30:20Z", "TSLA", "Tesla recalls vehicles"),  # aucun cours pour TSLA
            _article(5, "2026-10-13T18:00:20Z", "AAPL", "Apple supplier faces delay"),  # bien apres le choc
        ]
    }
    _write(tmp_path, "alpaca_news", "lot", "2026-10-13", "001", [news])
    _write(tmp_path, "alpaca_news", "lot", "2026-10-13", "002", [news])  # reprise
    return tmp_path


def test_replayed_archives_do_not_create_duplicates(data_dir):
    _, _, _, stats = run(data_dir, TICKERS)
    assert stats["news_lues"] == 5  # 5 articles, malgre les deux copies de chaque lot
    assert stats["barres"] == 2 * 780  # AAPL + SPY, une barre = (titre, minute)


def test_reposts_after_hours_and_missing_prices_are_not_results(data_dir):
    res, _, _, stats = run(data_dir, TICKERS)
    assert stats["evenements_en_seance"] == 3  # articles 1, 4 et 5 (2 = reprise, 3 = hors seance)
    assert set(res["article_id"]) == {"alpaca:1", "alpaca:5"}  # TSLA n'a pas de cours : pas de resultat


def test_shock_after_publication_is_detected_and_late_article_is_not(data_dir):
    res, _, _, _ = run(data_dir, TICKERS)
    car = res.set_index("article_id")["car_0_15"]
    assert car["alpaca:1"] > 0.002
    assert car["alpaca:1"] > car["alpaca:5"]


def test_run_is_deterministic(data_dir):
    # computed_at = heure du calcul : volatile par construction ; tout le reste doit etre identique
    first = run(data_dir, TICKERS)[0].drop(columns=["computed_at"])
    second = run(data_dir, TICKERS)[0].drop(columns=["computed_at"])
    pd.testing.assert_frame_equal(first, second)


def test_empty_archive_returns_empty_results(tmp_path):
    res, windows, err, stats = run(tmp_path, TICKERS)
    assert res.empty and windows.empty and err.empty
    assert stats == {"news_lues": 0, "evenements_en_seance": 0, "barres": 0}


def test_summary_counts_match_results(data_dir):
    res = run(data_dir, TICKERS)[0]
    assert summarize(res)["n_calcules"] == len(res)
