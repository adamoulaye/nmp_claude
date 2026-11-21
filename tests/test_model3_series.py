import numpy as np
import pandas as pd

from nmp.jobs_model3 import anomaly_rows, hourly_series, news_coverage


def _bars(n=3000, seed=1):
    rng = np.random.default_rng(seed)
    ts = pd.date_range("2026-10-05 13:30", periods=n, freq="min", tz="UTC")
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.0003, n)))
    return pd.DataFrame(
        {
            "hk_security": "A",
            "bar_ts": ts,
            "close": close,
            "high": close * 1.0005,
            "low": close * 0.9995,
            "volume": rng.integers(900, 1100, n).astype(float),
        }
    )


def test_anomaly_rows_flags_spike_and_explains_with_news():
    b = _bars()
    t = b["bar_ts"].iloc[2500]
    b.loc[2500:, "close"] *= 1.02
    b.loc[2500, ["high", "volume"]] = [b.loc[2500, "close"] * 1.02, 50000.0]
    news = pd.DataFrame({"hk_security": ["A"], "published_utc": [t - pd.Timedelta("2min")]})
    out = anomaly_rows(b, news, "iforest_test")
    assert list(out.columns) == ["hk_security", "bar_ts", "anomaly_score", "anomaly_type", "model_version"]
    row = out[out["bar_ts"] == t]
    assert len(row) == 1 and row["anomaly_type"].iloc[0] == "expliquee"
    assert (out["model_version"] == "iforest_test").all()


def test_anomaly_rows_skips_short_series():
    out = anomaly_rows(_bars(300), pd.DataFrame(columns=["hk_security", "published_utc"]), "v")
    assert out.empty


def test_hourly_series_ignores_overnight_gap():
    d1 = pd.date_range("2026-10-05 19:00", periods=60, freq="min", tz="UTC")
    d2 = pd.date_range("2026-10-06 13:30", periods=60, freq="min", tz="UTC")
    ts = d1.append(d2)
    close = np.concatenate([np.full(60, 100.0), np.full(60, 150.0)])  # saut de nuit énorme
    bars = pd.DataFrame({"hk_security": "A", "bar_ts": ts, "close": close})
    news = pd.DataFrame(
        {"published_utc": [pd.Timestamp("2026-10-05 19:10", tz="UTC")] * 2, "sentiment_score": [0.5, -0.1]}
    )
    sent, vol = hourly_series(bars, news)
    assert float(vol.max()) == 0.0
    assert abs(float(sent.iloc[0]) - 0.2) < 1e-9


def test_granger_pair_runs_and_detects_sentiment_leading_volatility():
    from nmp.ml.granger import granger_pair

    rng = np.random.default_rng(3)
    idx = pd.date_range("2026-10-01", periods=600, freq="h", tz="UTC")
    sent = pd.Series(rng.normal(0, 1, 600), index=idx)
    vol = pd.Series(0.8 * sent.shift(1).fillna(0).to_numpy() + rng.normal(0, 0.5, 600), index=idx)
    res = granger_pair(sent, vol, max_lag=3)
    assert len(res) == 6 and set(res["cause"]) == {"sent", "vol"}
    best = res[(res["cause"] == "sent") & (res["effect"] == "vol")]["p_bonf"].min()
    assert best < 0.01


def test_news_coverage_window_is_minus15_plus5():
    idx = pd.DatetimeIndex(pd.date_range("2026-10-05 14:00", periods=40, freq="min", tz="UTC"))
    cov = news_coverage(idx, pd.Series([pd.Timestamp("2026-10-05 14:20", tz="UTC")]))
    covered = idx[cov]
    assert covered.min() == pd.Timestamp("2026-10-05 14:15", tz="UTC")
    assert covered.max() == pd.Timestamp("2026-10-05 14:35", tz="UTC")


def test_baseline_stats_reports_base_rate_and_lifts():
    b = _bars()
    t = b["bar_ts"].iloc[2500]
    b.loc[2500:, "close"] *= 1.02
    b.loc[2500, ["high", "volume"]] = [b.loc[2500, "close"] * 1.02, 50000.0]
    news = pd.DataFrame({"hk_security": ["A"], "published_utc": [t - pd.Timedelta("2min")]})
    stats = []
    anomaly_rows(b, news, "v1", stats=stats)
    assert len(stats) == 1
    s = stats[0]
    assert s["hk_security"] == "A" and s["model_version"] == "v1"
    assert 0.0 <= s["base_rate_news"] < 0.1 and s["n_anomalies"] >= 1
    assert s["explained_rate_iforest"] > s["base_rate_news"]
    assert 0.0 <= s["overlap_zscore"] <= 1.0
