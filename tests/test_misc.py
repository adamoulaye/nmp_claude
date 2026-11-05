import numpy as np
import pandas as pd

from nmp.ml.drift import psi
from nmp.transform.tickers import associate, build_alias_patterns


def test_psi_stable_vs_derive():
    rng = np.random.default_rng(0)
    a = pd.Series(rng.normal(0, 1, 5000))
    assert psi(a, pd.Series(rng.normal(0, 1, 5000))) < 0.1
    assert psi(a, pd.Series(rng.normal(1.5, 1, 5000))) > 0.25


def test_association_ticker():
    pats = build_alias_patterns({"AAPL": ["Apple"], "TSLA": ["Tesla"]})
    res = {t: (c, m) for t, c, m in associate("Apple beats estimates", "Tesla also mentioned", ["MSFT"], pats)}
    assert res["MSFT"] == (1.0, "source") and res["AAPL"] == (0.9, "titre") and res["TSLA"] == (0.6, "corps")
