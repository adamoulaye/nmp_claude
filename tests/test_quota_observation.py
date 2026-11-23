"""Observation du quota API via les en-tetes X-RateLimit-* (sans reseau, sans Spark)."""

from nmp.common.retry import end_quota_observation, get_json, start_quota_observation
from nmp.common.runlog import run_log


class FakeResp:
    status_code = 200

    def __init__(self, headers):
        self.headers = headers

    def raise_for_status(self):
        return None

    def json(self):
        return {}


class FakeSession:
    def __init__(self, resps):
        self.resps = list(resps)

    def get(self, *a, **k):
        return self.resps.pop(0)


class FakeWriter:
    def __init__(self, spark, rows):
        self.spark, self.rows = spark, rows

    def mode(self, _):
        return self

    def saveAsTable(self, name):  # noqa: N802
        self.spark.saved[name] = self.rows


class FakeDF:
    def __init__(self, spark, rows):
        self.write = FakeWriter(spark, rows)


class FakeSpark:
    def __init__(self):
        self.saved = {}

    def createDataFrame(self, rows, schema=None):  # noqa: N802
        return FakeDF(self, rows)


def _hdr(limit, remaining):
    return {"X-RateLimit-Limit": str(limit), "X-RateLimit-Remaining": str(remaining)}


def test_observation_garde_le_minimum_restant():
    token = start_quota_observation()
    s = FakeSession([FakeResp(_hdr(60, 55)), FakeResp(_hdr(60, 40)), FakeResp(_hdr(60, 50))])
    for _ in range(3):
        get_json(s, "http://x")
    assert end_quota_observation(token) == {"limit": 60, "remaining": 40}


def test_sans_observation_pas_d_erreur():
    assert get_json(FakeSession([FakeResp(_hdr(60, 1))]), "http://x") == {}


def test_run_log_sans_spark_renseigne_quota_remaining():
    with run_log(None, "c", "job", "src") as st:
        get_json(FakeSession([FakeResp(_hdr(60, 12))]), "http://x")
    assert st.quota_remaining == 12


def test_sans_en_tetes_quota_reste_inconnu():
    with run_log(None, "c", "job", "src") as st:
        get_json(FakeSession([FakeResp({})]), "http://x")
    assert st.quota_remaining == -1


def test_run_log_ecrit_api_quota():
    spark = FakeSpark()
    with run_log(spark, "cat", "job", "finnhub"):
        get_json(FakeSession([FakeResp(_hdr(60, 45))]), "http://x")
    assert set(spark.saved) == {"cat.ops.run_log", "cat.ops.api_quota"}
    _, source, used, quota = spark.saved["cat.ops.api_quota"][0]
    assert (source, used, quota) == ("finnhub", 15, 60)


def test_pas_de_ligne_api_quota_sans_en_tetes():
    spark = FakeSpark()
    with run_log(spark, "cat", "job", "yahoo"):
        get_json(FakeSession([FakeResp({})]), "http://x")
    assert set(spark.saved) == {"cat.ops.run_log"}
