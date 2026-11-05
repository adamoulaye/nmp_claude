import numpy as np
import pandas as pd
import pytest
import requests

pytest.importorskip("sklearn")
from nmp.common.retry import QuotaExceeded, get_json  # noqa: E402
from nmp.ml.anomaly import detect  # noqa: E402


class FakeResp:
    def __init__(self, status, headers=None, body=None):
        self.status_code, self.headers, self._body = status, headers or {}, body or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, *resps):
        self.resps, self.calls = list(resps), 0

    def get(self, *a, **k):
        self.calls += 1
        return self.resps.pop(0)


def test_retry_sur_503_puis_succes(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    s = FakeSession(FakeResp(503), FakeResp(429, {"Retry-After": "1"}), FakeResp(200, body={"ok": 1}))
    assert get_json(s, "http://x") == {"ok": 1} and s.calls == 3


def test_quota_epuise_arret_immediat_sans_reprise():
    s = FakeSession(FakeResp(429, {"X-RateLimit-Remaining": "0"}))
    with pytest.raises(QuotaExceeded):
        get_json(s, "http://x")
    assert s.calls == 1


def _bars(spike_at, n=1200, seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2026-10-12 13:30", periods=n, freq="min", tz="UTC")
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.0003, n)))
    vol = rng.integers(900, 1100, n).astype(float)
    close[spike_at:] *= 1.05
    vol[spike_at] *= 40
    return pd.DataFrame({"close": close, "high": close * 1.0005, "low": close * 0.9995, "volume": vol}, index=idx)


def test_anomalie_expliquee_ou_non_selon_la_nouvelle():
    b = _bars(spike_at=1000)
    t = b.index[1000]
    avec = detect(b, pd.Series([t - pd.Timedelta(minutes=2)]))
    sans = detect(b, pd.Series([], dtype="datetime64[ns, UTC]"))
    assert avec.loc[t, "anomaly_type"] == "expliquee"
    assert sans.loc[t, "anomaly_type"] == "inexpliquee"
    assert not avec.loc[b.index[1100], "is_anomaly"]  # un calme plat n'est pas une anomalie
