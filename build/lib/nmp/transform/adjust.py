"""Ajustement des prix pour fractionnements et dividendes (méthode rétroactive, type CRSP)."""

from __future__ import annotations

import pandas as pd


def adjustment_factor(close: pd.Series, splits: pd.Series | None = None, dividends: pd.Series | None = None) -> pd.Series:
    """Facteur multiplicatif à appliquer à un prix brut de date d.

    close     : clôtures BRUTES indexées par date de séance
    splits    : ratio par date ex (2.0 pour un 2-pour-1)
    dividends : montant par date ex
    Facteur(d) = produit des facteurs d'événements dont la date ex est > d.
    """
    f = pd.Series(1.0, index=close.index)
    prev = close.shift(1)
    for dt, ratio in (splits.dropna() if splits is not None else pd.Series(dtype=float)).items():
        if dt in f.index and ratio and ratio > 0:
            f.loc[dt] *= 1.0 / ratio
    for dt, amt in (dividends.dropna() if dividends is not None else pd.Series(dtype=float)).items():
        if dt in f.index and pd.notna(prev.loc[dt]) and prev.loc[dt] > 0:
            f.loc[dt] *= 1.0 - amt / prev.loc[dt]
    return f.iloc[::-1].cumprod().iloc[::-1].shift(-1).fillna(1.0)


def adjust_intraday(
    bars: pd.DataFrame,
    daily_factor: pd.Series,
    ts_col="bar_ts",
    tz="America/New_York",
    *,
    split_factor: pd.Series | None = None,
) -> pd.DataFrame:
    """Applique le facteur de la séance aux barres 1 min.

    - prix (open/high/low/close/vwap) : × daily_factor (splits ET dividendes) ;
    - volume : ÷ split_factor SEULEMENT si fourni (facteur calculé avec `adjustment_factor(close, splits=...)`,
      sans dividendes : un dividende ne change pas le nombre d'actions échangées). Sinon le volume reste brut.
    """
    out = bars.copy()
    d = pd.to_datetime(out[ts_col], utc=True).dt.tz_convert(tz).dt.date

    def per_bar(series: pd.Series) -> pd.Series:
        return d.map({pd.Timestamp(k).date(): v for k, v in series.items()}).astype(float).fillna(1.0)

    f = per_bar(daily_factor)
    for c in ("open", "high", "low", "close", "vwap"):
        if c in out:
            out[c] = out[c] * f
    if split_factor is not None and "volume" in out:
        out["volume"] = out["volume"] / per_bar(split_factor)
    out["adj_factor"] = f
    return out
