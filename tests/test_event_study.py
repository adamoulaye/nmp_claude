
import numpy as np
import pandas as pd
import pytest

from nmp.transform.event_study import (
    IncompleteEventError,
    compute_event,
    cross_sectional_test,
    log_returns,
)


def make_bars(shock=0.0, seed=0):
    """Génère deux séances de données boursières synthétiques."""
    rng = np.random.default_rng(seed)

    days = [
        pd.date_range(
            f"2026-10-{d} 13:30",
            periods=390,
            freq="min",
            tz="UTC",
        )
        for d in (12, 13)
    ]

    idx = days[0].append(days[1])

    r_m = rng.normal(0, 0.0005, len(idx))
    r_s = 1.2 * r_m + rng.normal(0, 0.0003, len(idx))

    m0 = pd.Timestamp("2026-10-13 15:30", tz="UTC")

    if shock:
        r_s[
            (idx > m0)
            & (idx <= m0 + pd.Timedelta(minutes=5))
        ] += shock / 5

    def make_frame(returns, initial_price):
        return pd.DataFrame({
            "ts_utc": idx,
            "close": initial_price * np.exp(np.cumsum(returns)),
            "volume": rng.integers(100, 1000, len(idx)),
        })

    return (
        make_frame(r_s, 150.0),
        make_frame(r_m, 500.0),
    )


def test_log_returns_drops_overnight_gap():
    """Vérifie que les rendements ne traversent pas les nuits."""
    stock, _ = make_bars()

    returns = log_returns(
        stock.set_index("ts_utc")["close"]
    )

    assert returns.isna().sum() == 2


def test_shock_after_publication_is_detected():
    """Vérifie la détection d'un choc après la publication."""
    stock, market = make_bars(shock=0.004)

    event_time = pd.Timestamp(
        "2026-10-13 15:30:20",
        tz="UTC",
    )

    result, windows = compute_event(
        stock,
        market,
        event_time,
    )

    assert abs(result["beta"] - 1.2) < 0.1

    assert (
        result["car_0_15"] > 0.003
        and result["p_0_15"] < 0.01
    )

    assert abs(result["car_pre"]) < 0.004
    assert not result["leak_flag"]

    # La minute contenant la publication est exclue.
    assert windows.loc[
        windows.k == 0,
        "ar",
    ].isna().all()

    assert windows["k"].min() == -15
    assert windows["k"].max() == 60


def test_no_shock_is_not_significant():
    """Vérifie qu'un événement sans choc n'est pas significatif."""
    stock, market = make_bars(
        shock=0.0,
        seed=3,
    )

    event_time = pd.Timestamp(
        "2026-10-13 15:30:20",
        tz="UTC",
    )

    result, _ = compute_event(
        stock,
        market,
        event_time,
    )

    assert result["p_0_15"] > 0.01


def test_estimation_ignores_future_data():
    """Vérifie que le modèle de marché ignore les données futures."""
    stock, market = make_bars(shock=0.004)

    event_time = pd.Timestamp(
        "2026-10-13 15:30:20",
        tz="UTC",
    )

    result_a, _ = compute_event(
        stock,
        market,
        event_time,
    )

    stock_modified = stock.copy()
    market_modified = market.copy()

    mask = (
        stock_modified["ts_utc"]
        > event_time + pd.Timedelta(minutes=60)
    )

    stock_modified.loc[mask, "close"] *= 3

    result_b, _ = compute_event(
        stock_modified,
        market_modified,
        event_time,
    )

    assert result_a["beta"] == result_b["beta"]
    assert result_a["car_0_60"] == result_b["car_0_60"]


def test_incomplete_event_window_is_rejected():
    """Une fenêtre de 60 minutes incomplète ne doit pas produire de CAR."""
    stock, market = make_bars(shock=0.004)

    event_time = pd.Timestamp(
        "2026-10-13 15:30:20",
        tz="UTC",
    )

    # On coupe les données 25 minutes après la publication.
    cutoff = (
        event_time.floor("min")
        + pd.Timedelta(minutes=25)
    )

    stock = stock[
        stock["ts_utc"] <= cutoff
    ].copy()

    market = market[
        market["ts_utc"] <= cutoff
    ].copy()

    with pytest.raises(
        IncompleteEventError,
        match="incomplete",
    ):
        compute_event(
            stock,
            market,
            event_time,
        )


def test_cross_sectional():
    """Vérifie le test transversal des rendements anormaux."""
    output = cross_sectional_test(
        [0.01, 0.02, 0.015, 0.03, 0.012, 0.02, 0.018]
    )

    assert output["n"] == 7
    assert output["p_ttest"] < 0.01
