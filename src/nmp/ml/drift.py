"""Dérive : Population Stability Index (PSI) numérique et catégoriel."""

from __future__ import annotations

import numpy as np
import pandas as pd


def psi(expected: pd.Series, actual: pd.Series, bins: int = 10, eps: float = 1e-6) -> float:
    """PSI < 0.1 stable ; 0.1-0.25 à surveiller ; > 0.25 dérive significative."""
    e, a = expected.dropna(), actual.dropna()
    if e.dtype.kind in "OUSb" or e.dtype.name == "category":
        cats = sorted(set(e) | set(a))
        pe = e.value_counts(normalize=True).reindex(cats, fill_value=0) + eps
        pa = a.value_counts(normalize=True).reindex(cats, fill_value=0) + eps
    else:
        edges = np.unique(np.quantile(e, np.linspace(0, 1, bins + 1)))
        edges[0], edges[-1] = -np.inf, np.inf
        pe = pd.Series(np.histogram(e, edges)[0] / len(e)) + eps
        pa = pd.Series(np.histogram(a, edges)[0] / len(a)) + eps
    return float(((pa - pe) * np.log(pa / pe)).sum())
