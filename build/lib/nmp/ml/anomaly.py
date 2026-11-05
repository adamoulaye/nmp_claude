"""Modèle 3a : mouvements inhabituels (non supervisé) + attribution « expliqué par une nouvelle »."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _robust_z(s: pd.Series, window: int) -> pd.Series:
    med = s.rolling(window, min_periods=window // 4).median()
    mad = (s - med).abs().rolling(window, min_periods=window // 4).median() * 1.4826
    return (s - med) / mad.replace(0, np.nan)


def features(bars: pd.DataFrame, window: int = 390) -> pd.DataFrame:
    """bars : index bar_ts (UTC), colonnes close, high, low, volume, pour UN ticker."""
    b = bars.sort_index()
    f = pd.DataFrame(index=b.index)
    f["ret"] = np.log(b["close"]).diff()
    f["z_ret"] = _robust_z(f["ret"], window)
    f["z_vol"] = _robust_z(np.log1p(b["volume"]), window)
    f["z_range"] = _robust_z((b["high"] - b["low"]) / b["close"], window)
    return f.dropna()


def detect(
    bars: pd.DataFrame,
    news_ts: pd.Series,
    *,
    train_frac: float = 0.6,
    contamination: float = 0.002,
    news_window: tuple[str, str] = ("5min", "15min"),
    seed: int = 0,
) -> pd.DataFrame:
    """IsolationForest entraîné sur le début de la période (pas de fuite), puis scoring du reste.

    news_ts : horodatages UTC de publication des nouvelles du ticker (les 1res publications).
    Une anomalie est « expliquée » si une nouvelle a été publiée entre -15 min et +5 min de la barre.
    """
    from sklearn.ensemble import IsolationForest

    f = features(bars)
    cols = ["z_ret", "z_vol", "z_range"]
    n_tr = int(len(f) * train_frac)
    iso = IsolationForest(n_estimators=300, contamination=contamination, random_state=seed, n_jobs=-1).fit(f.iloc[:n_tr][cols])
    out = f.iloc[n_tr:].copy()
    out["anomaly_score"] = -iso.score_samples(out[cols])
    out["is_anomaly"] = iso.predict(out[cols]) == -1
    nts = pd.DatetimeIndex(pd.to_datetime(news_ts, utc=True)).sort_values()
    after, before = pd.Timedelta(news_window[0]), pd.Timedelta(news_window[1])
    idx = nts.searchsorted(out.index - before)  # première nouvelle >= t - 15 min
    has = (
        (idx < len(nts)) & (nts[np.clip(idx, 0, max(len(nts) - 1, 0))] <= out.index + after)
        if len(nts)
        else np.zeros(len(out), bool)
    )
    out["explained_by_news"] = np.asarray(has) & out["is_anomaly"].to_numpy()
    out["anomaly_type"] = np.select(
        [out.is_anomaly & out.explained_by_news, out.is_anomaly], ["expliquee", "inexpliquee"], "normal"
    )
    return out
