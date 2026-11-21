import pytest

from nmp.ml.registry import decide_promotion, promote_if_better


def test_no_champion_promotes():
    assert decide_promotion(0.4, None)[0] is True


def test_better_with_margin_promotes():
    assert decide_promotion(0.52, 0.50, 0.01)[0] is True


def test_better_below_margin_refused():
    ok, why = decide_promotion(0.505, 0.50, 0.01)
    assert ok is False and "refuse" in why


def test_equal_refused_and_worse_refused():
    assert decide_promotion(0.5, 0.5, 0.01)[0] is False
    assert decide_promotion(0.4, 0.5, 0.01)[0] is False


class _MV:
    def __init__(self, run_id, version):
        self.run_id, self.version = run_id, version


class _Run:
    def __init__(self, metrics):
        class D:
            pass

        self.data = D()
        self.data.metrics = metrics


class FakeClient:
    def __init__(self, versions, runs, aliases=None):
        self.versions, self.runs, self.aliases, self.calls = versions, runs, dict(aliases or {}), []

    def get_model_version(self, name, version):
        return _MV(self.versions[version], version)

    def get_model_version_by_alias(self, name, alias):
        if alias not in self.aliases:
            raise KeyError(alias)
        v = self.aliases[alias]
        return _MV(self.versions[v], v)

    def get_run(self, run_id):
        return _Run(self.runs[run_id])

    def set_registered_model_alias(self, name, alias, version):
        self.aliases[alias] = version
        self.calls.append((alias, version))


def _client(champ_f1, chall_f1, with_champion=True):
    return FakeClient(
        versions={"1": "r1", "2": "r2"},
        runs={"r1": {"holdout_macro_f1": champ_f1}, "r2": {"holdout_macro_f1": chall_f1}},
        aliases={"champion": "1"} if with_champion else {},
    )


def test_promote_moves_alias_when_better():
    c = _client(0.50, 0.55)
    assert promote_if_better("m", "2", client=c) is True
    assert c.aliases["champion"] == "2" and c.aliases["challenger"] == "2"


def test_refused_keeps_champion_but_sets_challenger():
    c = _client(0.50, 0.505)
    assert promote_if_better("m", "2", client=c) is False
    assert c.aliases["champion"] == "1" and c.aliases["challenger"] == "2"


def test_dry_run_changes_nothing():
    c = _client(0.50, 0.60)
    assert promote_if_better("m", "2", client=c, dry_run=True) is True
    assert c.calls == [] and c.aliases == {"champion": "1"}


def test_first_ever_champion():
    c = _client(0.0, 0.40, with_champion=False)
    assert promote_if_better("m", "2", client=c) is True
    assert c.aliases["champion"] == "2"


def test_missing_metric_raises():
    c = FakeClient({"2": "r2"}, {"r2": {}})
    with pytest.raises(ValueError):
        promote_if_better("m", "2", client=c)
