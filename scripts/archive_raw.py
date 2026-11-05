"""Archive les reponses BRUTES de toutes les sources (Bronze, ajout seulement).
A planifier toutes les 30 minutes. Chaque execution est journalisee (data/logs/runs.jsonl).
Usage: python scripts/archive_raw.py --lookback-hours 6"""

import argparse
from datetime import datetime, timedelta, timezone

from nmp.local.bronze import log_run, save_raw
from nmp.local.config import BENCHMARK, PILOT_TICKERS
from nmp.local.sources import alpaca, finnhub, sec


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def count_pages(pages):
    n = 0
    for p in pages:
        n += len(p.get("news") or []) + sum(len(v) for v in (p.get("bars") or {}).values())
    return n


def job(source, key, fetch, counter=len, params=None):
    started = datetime.now(timezone.utc)
    try:
        payload = fetch()
        save_raw(source, key, payload, params)
        log_run(source, key, counter(payload), started)
        print(f"ok   {source}/{key}: {counter(payload)}")
    except Exception as e:  # noqa: BLE001  (une source en panne ne doit pas bloquer les autres)
        log_run(source, key, 0, started, status="error", error=f"{type(e).__name__}: {e}"[:300])
        print(f"FAIL {source}/{key}: {e}")


def main(lookback_hours: int):
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=lookback_hours)
    for t in PILOT_TICKERS:
        job(
            "finnhub",
            t,
            lambda t=t: finnhub.fetch_company_news(t, start.date().isoformat(), now.date().isoformat()),
        )
    job("alpaca_news", "ALL", lambda: alpaca.fetch_news(PILOT_TICKERS, iso(start), iso(now)), count_pages)
    job(
        "alpaca_bars",
        "ALL",
        lambda: alpaca.fetch_bars(PILOT_TICKERS + [BENCHMARK], iso(start), iso(now)),
        count_pages,
    )
    ciks = sec.ticker_to_cik()
    for t in PILOT_TICKERS:
        job("sec_8k", t, lambda t=t: sec.fetch_submissions(ciks[t]), counter=lambda p: 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback-hours", type=int, default=6)
    main(ap.parse_args().lookback_hours)
