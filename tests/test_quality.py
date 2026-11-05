import pandas as pd

from nmp.local.quality import pipeline_success_rate, silent_sources


def runs(rows):
    df = pd.DataFrame(rows, columns=["source", "status", "started_at"])
    df["started_at"] = pd.to_datetime(df["started_at"], utc=True)
    return df


def test_silent_source_only_when_market_open():
    r = runs(
        [
            ("alpaca_bars", "ok", "2026-10-13 14:00"),
            ("finnhub", "ok", "2026-10-13 15:00"),
            ("alpaca_news", "ok", "2026-10-13 15:00"),
            ("sec_8k", "ok", "2026-10-13 15:00"),
        ]
    )
    open_now = pd.Timestamp("2026-10-13 15:05", tz="UTC")  # seance reguliere
    assert [a["source"] for a in silent_sources(r, open_now)] == ["alpaca_bars"]
    closed_now = pd.Timestamp("2026-10-17 15:05", tz="UTC")  # samedi
    assert silent_sources(r, closed_now) == []


def test_success_rate():
    r = runs([("a", "ok", "2026-10-13 10:00"), ("a", "error", "2026-10-13 11:00")])
    assert pipeline_success_rate(r, 24, pd.Timestamp("2026-10-13 12:00", tz="UTC")) == 0.5


def test_check_bars_detects_violations():
    from nmp.local.quality import check_bars

    t = pd.Timestamp("2026-10-13 14:00", tz="UTC")
    bars = pd.DataFrame(
        {
            "ticker": ["A", "A", "B"],
            "ts_utc": [t, t, t],
            "open": [10, 10, -1],
            "high": [11, 11, 2],
            "low": [9, 9, 1],
            "close": [10, 10, 1],
            "volume": [5, 5, -3],
        }
    )
    out = check_bars(bars)
    assert out["doublons_ticker_minute"] == 1 and out["prix_non_positifs"] == 1
    assert out["volume_negatif"] == 1 and out["horodatage_non_utc"] == 0


def test_text_price_desync():
    from nmp.local.quality import text_price_desync

    now = pd.Timestamp("2026-10-13 15:00", tz="UTC")
    bar, news = now - pd.Timedelta(minutes=1), now - pd.Timedelta(minutes=2)
    assert not text_price_desync(news, bar, now=now)
    assert text_price_desync(now - pd.Timedelta(minutes=40), bar, now=now)
    sat = pd.Timestamp("2026-10-17 15:00", tz="UTC")  # samedi: jamais d'alerte
    assert not text_price_desync(sat - pd.Timedelta(hours=9), sat - pd.Timedelta(hours=9), now=sat)


def test_silent_source_demo_flag_ignores_market_hours():
    r = runs(
        [
            ("alpaca_bars", "ok", "2026-10-16 10:00"),
            ("finnhub", "ok", "2026-10-16 10:00"),
            ("alpaca_news", "ok", "2026-10-16 10:00"),
            ("sec_8k", "ok", "2026-10-16 10:00"),
        ]
    )
    sat = pd.Timestamp("2026-10-17 15:05", tz="UTC")
    assert silent_sources(r, sat) == []
    assert len(silent_sources(r, sat, ignore_market_hours=True)) == 4
