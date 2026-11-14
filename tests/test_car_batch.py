
import sys

import pandas as pd

sys.path.insert(0, "tests")

from test_event_study import make_bars

from nmp.transform.car_batch import (
    run_batch,
    to_bv_event_car,
)


def _make_batch_data(shock=0.004):
    """Prépare les données de test pour AAPL et SPY."""
    stock, market = make_bars(shock=shock)

    bars = pd.concat(
        [
            stock.assign(ticker="AAPL"),
            market.assign(ticker="SPY"),
        ],
        ignore_index=True,
    )

    event_time = pd.Timestamp("2026-10-13 15:30:20", tz="UTC")

    return bars, event_time


def test_run_batch_results_and_errors():
    """Vérifie le calcul CAR et la gestion d'un titre sans données."""
    bars, event_time = _make_batch_data()

    news = pd.DataFrame({
        "article_id": ["a1", "a2"],
        "ticker": ["AAPL", "TSLA"],
        "published_utc": [event_time, event_time],
    })

    results, windows, errors = run_batch(news, bars)

    assert list(results["article_id"]) == ["a1"]
    assert results.loc[0, "p_0_15"] < 0.01

    assert set(windows["article_id"]) == {"a1"}
    assert len(windows) == 76

    assert list(errors["article_id"]) == ["a2"]
    assert "TSLA" in errors.loc[0, "error"]

    if "status" in errors.columns:
        assert errors.loc[0, "status"] == "error"


def test_run_batch_preserves_event_time_basis():
    """Vérifie que run_batch conserve la référence temporelle."""
    bars, event_time = _make_batch_data()

    news = pd.DataFrame({
        "hk_article": ["a1", "a1"],
        "hk_security": ["s1", "s1"],
        "ticker": ["AAPL", "AAPL"],
        "event_time_basis": ["published", "available"],
        "event_utc": [event_time, event_time],
    })

    results, _, errors = run_batch(
        news,
        bars,
        id_col="hk_article",
        ts_col="event_utc",
        keep=("hk_security", "event_time_basis"),
    )

    assert errors.empty
    assert len(results) == 2

    assert set(results["event_time_basis"]) == {
        "published",
        "available",
    }

    assert set(results["hk_security"]) == {"s1"}


def test_run_batch_marks_incomplete_event_as_pending():
    """Un événement sans 60 minutes de données doit rester en attente."""
    bars, event_time = _make_batch_data()

    # On conserve les données jusqu'à 25 minutes après la publication.
    # La fenêtre CAR de 60 minutes n'est donc pas complète.
    cutoff = event_time.floor("min") + pd.Timedelta(minutes=25)

    bars = bars[
        bars["ts_utc"] <= cutoff
    ].copy()

    news = pd.DataFrame({
        "hk_article": ["a1"],
        "hk_security": ["s1"],
        "ticker": ["AAPL"],
        "event_time_basis": ["published"],
        "event_utc": [event_time],
    })

    results, windows, errors = run_batch(
        news,
        bars,
        id_col="hk_article",
        ts_col="event_utc",
        keep=("hk_security", "event_time_basis"),
    )

    assert results.empty
    assert windows.empty
    assert not errors.empty

    assert list(errors["hk_article"]) == ["a1"]
    assert "status" in errors.columns
    assert errors.loc[0, "status"] == "pending"

    assert "incomplete" in errors.loc[0, "error"].lower()


def test_to_bv_event_car_creates_two_windows():
    """Vérifie la transformation des CAR en deux fenêtres."""
    computed_at = pd.Timestamp("2026-11-09T20:00:00Z")

    results = pd.DataFrame({
        "hk_article": ["a1"],
        "hk_security": ["s1"],
        "event_time_basis": ["published"],
        "car_0_15": [0.01],
        "car_0_60": [0.03],
        "t_0_15": [2.0],
        "t_0_60": [2.5],
        "p_0_15": [0.04],
        "p_0_60": [0.01],
        "alpha": [0.001],
        "beta": [1.1],
        "n_est": [390],
        "car_pre": [0.002],
        "leak_flag": [False],
        "computed_at": [computed_at],
    })

    output = to_bv_event_car(results)

    assert len(output) == 2
    assert set(output["window_end_min"]) == {15, 60}
    assert set(output["window_start_min"]) == {1}
    assert set(output["event_time_basis"]) == {"published"}

    car_15 = output[output["window_end_min"] == 15].iloc[0]
    car_60 = output[output["window_end_min"] == 60].iloc[0]

    assert car_15["car"] == 0.01
    assert car_15["t_stat"] == 2.0
    assert car_15["p_value"] == 0.04

    assert car_60["car"] == 0.03
    assert car_60["t_stat"] == 2.5
    assert car_60["p_value"] == 0.01

    assert car_15["model_version"] == car_60["model_version"]


def test_to_bv_event_car_preserves_event_time_basis():
    """Vérifie que publication et disponibilité restent distinctes."""
    computed_at = pd.Timestamp("2026-11-09T20:00:00Z")

    results = pd.DataFrame({
        "hk_article": ["a1", "a1"],
        "hk_security": ["s1", "s1"],
        "event_time_basis": ["published", "available"],
        "car_0_15": [0.01, 0.02],
        "car_0_60": [0.03, 0.04],
        "t_0_15": [2.0, 2.1],
        "t_0_60": [2.5, 2.6],
        "p_0_15": [0.04, 0.03],
        "p_0_60": [0.01, 0.02],
        "alpha": [0.001, 0.001],
        "beta": [1.1, 1.1],
        "n_est": [390, 390],
        "car_pre": [0.0, 0.0],
        "leak_flag": [False, False],
        "computed_at": [computed_at, computed_at],
    })

    output = to_bv_event_car(results)

    assert len(output) == 4

    assert set(output["event_time_basis"]) == {
        "published",
        "available",
    }

    published = output[
        output["event_time_basis"] == "published"
    ]

    available = output[
        output["event_time_basis"] == "available"
    ]

    assert set(published["window_end_min"]) == {15, 60}
    assert set(available["window_end_min"]) == {15, 60}

    assert set(published["car"]) == {0.01, 0.03}
    assert set(available["car"]) == {0.02, 0.04}


def test_to_bv_event_car_empty_input():
    """Vérifie qu'une entrée vide retourne un DataFrame vide."""
    output = to_bv_event_car(pd.DataFrame())

    assert output.empty


def test_to_bv_event_car_rejects_missing_columns():
    """Vérifie qu'une entrée incomplète produit une erreur explicite."""
    results = pd.DataFrame({
        "hk_article": ["a1"],
        "hk_security": ["s1"],
    })

    try:
        to_bv_event_car(results)
    except ValueError as exc:
        assert "Colonnes manquantes" in str(exc)
    else:
        raise AssertionError(
            "Une ValueError était attendue pour les colonnes manquantes."
        )
