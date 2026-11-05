import pandas as pd
import pytest

pytest.importorskip("deltalake")
pytest.importorskip("pyarrow")


def test_aller_retour_delta_et_fuseau(tmp_path, monkeypatch):
    from nmp.local import delta_io

    monkeypatch.setattr(delta_io, "DATA_DIR", tmp_path)
    df = pd.DataFrame(
        {"k": ["a", "b"], "ts": pd.to_datetime(["2026-10-13 14:00", "2026-10-13 15:00"], utc=True), "v": [1.5, 2.5]}
    )
    delta_io.to_delta(df, "t", mode="overwrite")
    delta_io.to_delta(df, "t", mode="append")
    out = delta_io.read_delta("t")
    assert len(out) == 4 and str(out["ts"].dt.tz) == "UTC" and out["v"].sum() == 8.0
