"""Alpaca Market Data: nouvelles (Benzinga) et barres d'une minute. Cle API gratuite requise."""

import pandas as pd

from ..config import ALPACA_KEY_ID, ALPACA_SECRET
from ..http_utils import make_session
from ..schema import BAR_COLS, NEWS_COLS

BASE = "https://data.alpaca.markets"


def _session():
    return make_session({"APCA-API-KEY-ID": ALPACA_KEY_ID, "APCA-API-SECRET-KEY": ALPACA_SECRET})


def _paginate(s, path: str, params: dict) -> list[dict]:
    """Suit next_page_token jusqu'a epuisement. Retourne la liste des pages brutes."""
    pages, token = [], None
    while True:
        p = dict(params)
        if token:
            p["page_token"] = token
        r = s.get(f"{BASE}{path}", params=p, timeout=60)
        r.raise_for_status()
        j = r.json()
        pages.append(j)
        token = j.get("next_page_token")
        if not token:
            return pages


def fetch_news(symbols: list[str], start: str, end: str, session=None) -> list[dict]:
    """start / end: RFC-3339 UTC, ex. '2026-10-13T00:00:00Z'."""
    params = {
        "symbols": ",".join(symbols),
        "start": start,
        "end": end,
        "limit": 50,
        "sort": "asc",
        "include_content": "false",
    }
    return _paginate(session or _session(), "/v1beta1/news", params)


def fetch_bars(
    symbols: list[str], start: str, end: str, feed: str = "iex", adjustment: str = "split", session=None
) -> list[dict]:
    """feed='iex' (gratuit, volumes partiels) ou 'sip' si votre plan l'autorise."""
    params = {
        "symbols": ",".join(symbols),
        "timeframe": "1Min",
        "start": start,
        "end": end,
        "limit": 10000,
        "feed": feed,
        "adjustment": adjustment,
        "sort": "asc",
    }
    return _paginate(session or _session(), "/v2/stocks/bars", params)


def normalize_news(pages: list[dict], tickers=None, ingested_utc=None) -> pd.DataFrame:
    ingested = ingested_utc or pd.Timestamp.now(tz="UTC")
    rows = []
    for page in pages:
        for a in page.get("news") or []:
            for sym in a.get("symbols") or []:
                if tickers and sym not in tickers:
                    continue
                rows.append(
                    {
                        "article_id": f"alpaca:{a['id']}",
                        "source": "alpaca",
                        "publisher": a.get("source"),
                        "ticker": sym,
                        "published_utc": pd.to_datetime(a["created_at"], utc=True),
                        "updated_utc": pd.to_datetime(a.get("updated_at"), utc=True),
                        "ingested_utc": ingested,
                        "headline": a.get("headline", ""),
                        "summary": a.get("summary", ""),
                        "url": a.get("url", ""),
                    }
                )
    return pd.DataFrame(rows, columns=NEWS_COLS)


def normalize_bars(pages: list[dict]) -> pd.DataFrame:
    rows = []
    for page in pages:
        for sym, bars in (page.get("bars") or {}).items():
            for b in bars:
                rows.append(
                    {
                        "ticker": sym,
                        "ts_utc": pd.to_datetime(b["t"], utc=True),
                        "open": b["o"],
                        "high": b["h"],
                        "low": b["l"],
                        "close": b["c"],
                        "volume": b["v"],
                        "trade_count": b.get("n"),
                        "vwap": b.get("vw"),
                    }
                )
    df = pd.DataFrame(rows, columns=BAR_COLS)
    # Idempotence: une barre = (ticker, minute). Les doublons de reprise sont ecartes ici.
    return df.drop_duplicates(["ticker", "ts_utc"]).sort_values(["ticker", "ts_utc"]).reset_index(drop=True)
