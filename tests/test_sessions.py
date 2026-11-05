import pandas as pd
import pytest

pytest.importorskip("pandas_market_calendars")
from nmp.common.timeutil import attach_session, build_sessions  # noqa: E402

S = build_sessions("2024-03-01", "2024-07-31")


def _run(ts):
    df = pd.DataFrame({"published_utc": pd.to_datetime([ts], utc=True)})
    return attach_session(df, S).iloc[0]


def test_pendant_reaction_immediate():
    r = _run("2024-03-11 15:00")  # 11:00 EDT
    assert r.session_type == "pendant" and r.reaction_start_utc == pd.Timestamp("2024-03-11 15:00", tz="UTC")


def test_avant_ouverture_dst():
    r = _run("2024-03-11 12:00")  # 08:00 EDT, DST a débuté le 10 mars -> ouverture 13:30 UTC
    assert r.session_type == "avant" and r.reaction_start_utc == pd.Timestamp("2024-03-11 13:30", tz="UTC")


def test_apres_cloture_vendredi_vers_lundi():
    r = _run("2024-03-08 21:30")  # 16:30 EST, vendredi
    assert r.session_type == "apres" and r.reaction_start_utc == pd.Timestamp("2024-03-11 13:30", tz="UTC")


def test_ferie_reaction_ouverture_suivante():
    r = _run("2024-07-04 12:00")
    assert r.session_type == "ferme" and r.reaction_start_utc == pd.Timestamp("2024-07-05 13:30", tz="UTC")


def test_weekend():
    assert _run("2024-03-09 18:00").session_type == "ferme"
