import numpy as np
import pandas as pd

from nmp.transform.event_study import compute_event, cross_sectional_test, log_returns


def make_bars(shock=0.0, seed=0):
    rng = np.random.default_rng(seed)
    days = [pd.date_range(f"2026-10-{d} 13:30", periods=390, freq="min", tz="UTC") for d in (12, 13)]
    idx = days[0].append(days[1])
    r_m = rng.normal(0, 0.0005, len(idx))
    r_s = 1.2 * r_m + rng.normal(0, 0.0003, len(idx))
    m0 = pd.Timestamp("2026-10-13 15:30", tz="UTC")
    if shock:
        r_s[(idx > m0) & (idx <= m0 + pd.Timedelta(minutes=5))] += shock / 5
    mk = lambda r, p0: pd.DataFrame(
        {"ts_utc": idx, "close": p0 * np.exp(np.cumsum(r)), "volume": rng.integers(100, 1000, len(idx))}
    )
    return mk(r_s, 150.0), mk(r_m, 500.0)


def test_log_returns_drops_overnight_gap():
    s, _ = make_bars()
    r = log_returns(s.set_index("ts_utc")["close"])
    assert r.isna().sum() == 2  # 1er point + rendement overnight


def test_shock_after_publication_is_detected():
    s, m = make_bars(shock=0.004)
    res, w = compute_event(s, m, pd.Timestamp("2026-10-13 15:30:20", tz="UTC"))
    assert abs(res["beta"] - 1.2) < 0.1
    assert res["car_0_15"] > 0.003 and res["p_0_15"] < 0.01
    assert abs(res["car_pre"]) < 0.004 and not res["leak_flag"]
    assert w.loc[w.k == 0, "ar"].isna().all()  # minute de publication exclue
    assert w["k"].min() == -15 and w["k"].max() == 60


def test_no_shock_is_not_significant():
    s, m = make_bars(shock=0.0, seed=3)
    res, _ = compute_event(s, m, pd.Timestamp("2026-10-13 15:30:20", tz="UTC"))
    assert res["p_0_15"] > 0.01


def test_estimation_ignores_future_data():
    s, m = make_bars(shock=0.004)
    pub = pd.Timestamp("2026-10-13 15:30:20", tz="UTC")
    a, _ = compute_event(s, m, pub)
    s2, m2 = s.copy(), m.copy()
    mask = s2["ts_utc"] > pub + pd.Timedelta(minutes=60)
    s2.loc[mask, "close"] *= 3  # on corrompt le futur lointain
    b, _ = compute_event(s2, m2, pub)
    assert a["beta"] == b["beta"] and a["car_0_60"] == b["car_0_60"]


def test_cross_sectional():
    out = cross_sectional_test([0.01, 0.02, 0.015, 0.03, 0.012, 0.02, 0.018])
    assert out["n"] == 7 and out["p_ttest"] < 0.01
