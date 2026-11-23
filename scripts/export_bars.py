"""Collecte les barres 1 min Alpaca depuis le poste local (IEX par defaut, --feed sip pour SIP) et les ecrit en JSONL."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

URL = "https://data.alpaca.markets/v2/stocks/bars"


def _load_env(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.split("#", 1)[0].strip().strip('"').strip("'"))


def _keys() -> tuple[str, str]:
    kid = os.getenv("ALPACA_KEY_ID") or os.getenv("ALPACA_API_KEY")
    sec = os.getenv("ALPACA_SECRET") or os.getenv("ALPACA_SECRET_KEY")
    if not kid or not sec:
        sys.exit("Clés Alpaca introuvables (ALPACA_KEY_ID / ALPACA_SECRET dans l'environnement ou .env).")
    return kid, sec


def fetch(session, symbols, start, end, feed="iex"):
    params = {
        "symbols": ",".join(symbols),
        "timeframe": "1Min",
        "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "feed": feed,
        "adjustment": "raw",
        "limit": 10000,
    }
    while True:
        for attempt in range(5):
            r = session.get(URL, params=params, timeout=30)
            if r.status_code == 429:
                time.sleep(2**attempt)
                continue
            break
        r.raise_for_status()
        body = r.json()
        for sym, bars in (body.get("bars") or {}).items():
            for b in bars:
                yield sym, b
        token = body.get("next_page_token")
        if not token:
            return
        params["page_token"] = token


def to_line(sym, b):
    row = {k: b.get(k) for k in ("t", "o", "h", "l", "c", "v", "vw", "n")}
    row["_symbol"] = sym.upper()
    return json.dumps(row, separators=(",", ":"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", required=True)
    ap.add_argument("--days", type=int, default=4)
    ap.add_argument("--out", default="bars.jsonl")
    ap.add_argument("--feed", default="iex")
    a = ap.parse_args()
    _load_env()
    kid, sec = _keys()
    symbols = [s.strip().upper() for s in a.symbols.split(",") if s.strip()]
    end = datetime.now(timezone.utc) - timedelta(minutes=16)
    start = end - timedelta(days=a.days)
    s = requests.Session()
    s.headers.update({"APCA-API-KEY-ID": kid, "APCA-API-SECRET-KEY": sec})
    n, per = 0, {}
    with open(a.out, "w", encoding="utf-8") as f:
        for sym, b in fetch(s, symbols, start, end, a.feed):
            f.write(to_line(sym, b) + "\n")
            n += 1
            per[sym] = per.get(sym, 0) + 1
    print(f"{n} barres écrites dans {a.out}")
    for sym in sorted(per):
        print(f"  {sym}: {per[sym]}")
    missing = [x for x in symbols if x not in per]
    if missing:
        print("Aucune barre pour :", ", ".join(missing))


if __name__ == "__main__":
    main()
