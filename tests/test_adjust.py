import pandas as pd

from nmp.transform.adjust import adjustment_factor


def test_split_2_pour_1():
    idx = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]).date
    close = pd.Series([100.0, 100.0, 50.0, 50.0], index=idx)
    f = adjustment_factor(close, splits=pd.Series({idx[2]: 2.0}))
    adj = close * f
    assert adj.round(6).tolist() == [50.0, 50.0, 50.0, 50.0]


def test_dividende():
    idx = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]).date
    close = pd.Series([100.0, 100.0, 99.0], index=idx)
    f = adjustment_factor(close, dividends=pd.Series({idx[2]: 1.0}))  # div 1$ sur 100$ -> facteur 0.99
    assert abs(f.iloc[0] - 0.99) < 1e-12 and abs(f.iloc[1] - 0.99) < 1e-12 and f.iloc[2] == 1.0
