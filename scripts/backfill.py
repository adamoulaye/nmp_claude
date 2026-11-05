"""Rattrapage de l'historique (a faire des le jour 2: c'est ce qui donne assez d'evenements).
Usage: python scripts/backfill.py --start 2026-07-01 --end 2026-10-14"""

import argparse
from datetime import date, datetime, timedelta, timezone

from nmp.local.bronze import log_run, save_raw
from nmp.local.config import BENCHMARK, PILOT_TICKERS
from nmp.local.sources import alpaca, finnhub


def chunks(start: date, end: date, days: int):
    cur = start
    while cur <= end:
        yield cur, min(cur + timedelta(days=days - 1), end)
        cur += timedelta(days=days)


def z(d: date, end_of_day=False):
    return f"{d.isoformat()}T{'23:59:59' if end_of_day else '00:00:00'}Z"


def main(start: date, end: date):
    for a, b in chunks(start, end, 7):  # nouvelles: fenetres de 7 jours
        for t in PILOT_TICKERS:
            t0 = datetime.now(timezone.utc)
            items = finnhub.fetch_company_news(t, a.isoformat(), b.isoformat())
            save_raw("finnhub", t, items, {"from": str(a), "to": str(b)})
            log_run("finnhub", t, len(items), t0)
        t0 = datetime.now(timezone.utc)
        pages = alpaca.fetch_news(PILOT_TICKERS, z(a), z(b, True))
        save_raw("alpaca_news", "ALL", pages, {"from": str(a), "to": str(b)})
        log_run("alpaca_news", "ALL", sum(len(p.get("news") or []) for p in pages), t0)
    for a, b in chunks(start, end, 3):  # barres minute: fenetres de 3 jours
        t0 = datetime.now(timezone.utc)
        pages = alpaca.fetch_bars(PILOT_TICKERS + [BENCHMARK], z(a), z(b, True))
        save_raw("alpaca_bars", "ALL", pages, {"from": str(a), "to": str(b)})
        n = sum(len(v) for p in pages for v in (p.get("bars") or {}).values())
        log_run("alpaca_bars", "ALL", n, t0)
        print(f"barres {a} -> {b}: {n}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=date.fromisoformat, required=True)
    ap.add_argument("--end", type=date.fromisoformat, required=True)
    args = ap.parse_args()
    main(args.start, args.end)
