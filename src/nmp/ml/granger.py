"""Modèle 3b : causalité de Granger sentiment agrégé -> volatilité (et sens inverse)."""

from __future__ import annotations

import warnings

import pandas as pd


def _stationary(s: pd.Series, alpha: float = 0.05) -> pd.Series:
    from statsmodels.tsa.stattools import adfuller

    s = s.dropna()
    return s if adfuller(s, autolag="AIC")[1] < alpha else s.diff().dropna()


def granger_pair(sentiment: pd.Series, volatility: pd.Series, max_lag: int = 6) -> pd.DataFrame:
    """Teste les deux sens sur séries rendues stationnaires (ADF). p corrigées Bonferroni sur les lags."""
    from statsmodels.tsa.stattools import grangercausalitytests

    d = pd.concat({"sent": _stationary(sentiment), "vol": _stationary(volatility)}, axis=1).dropna()
    rows = []
    for cause, effect in (("sent", "vol"), ("vol", "sent")):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            res = grangercausalitytests(d[[effect, cause]], maxlag=max_lag, verbose=False)
        for lag, r in res.items():
            p = r[0]["ssr_ftest"][1]
            rows.append({"cause": cause, "effect": effect, "lag": lag, "p_value": p, "p_bonf": min(1.0, p * max_lag)})
    return pd.DataFrame(rows)
