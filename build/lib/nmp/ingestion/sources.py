"""Connecteurs batch. Chaque source stocke le JSON brut ; le parsing vit en Silver."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

import pandas as pd

from nmp.common.retry import get_json
from nmp.ingestion.base import BaseSource, Record


def _dt(x) -> datetime | None:
    t = pd.to_datetime(x, utc=True, errors="coerce")
    return None if pd.isna(t) else t.to_pydatetime()


class FinnhubNews(BaseSource):
    name = "finnhub_news"
    expected_keys = frozenset({"id", "headline", "datetime"})

    def fetch(self, since, until) -> Iterator[dict]:
        for sym in self.s.tickers:
            rows = get_json(
                self.http,
                "https://finnhub.io/api/v1/company-news",
                params={
                    "symbol": sym,
                    "from": since.date().isoformat(),
                    "to": until.date().isoformat(),
                    "token": self.s.finnhub_key,
                },
            )
            for r in rows:
                r["_symbol"] = sym
                yield r

    def to_record(self, raw):
        return Record(
            self.name,
            f"{raw['id']}:{raw['_symbol']}",
            raw["_symbol"],
            _dt(pd.to_datetime(raw["datetime"], unit="s", utc=True)),
            None,
            raw,
        )


class FinnhubSentiment(BaseSource):
    """Sentiment pré-calculé Finnhub = référence de comparaison pour FinBERT (instantané quotidien)."""

    name = "finnhub_sentiment"
    expected_keys = frozenset({"symbol"})

    def fetch(self, since, until):
        for sym in self.s.tickers:
            r = get_json(
                self.http,
                "https://finnhub.io/api/v1/news-sentiment",
                params={"symbol": sym, "token": self.s.finnhub_key},
            )
            r["_asof"] = datetime.now(timezone.utc).isoformat()
            yield r

    def to_record(self, raw):
        ts = _dt(raw["_asof"])
        return Record(self.name, f"{raw['symbol']}:{ts:%Y%m%d}", raw["symbol"], ts, None, raw)


class AlpacaNews(BaseSource):
    """Historique/rattrapage REST ; le temps réel passe par streaming/streamer.py."""

    name = "alpaca_news"
    expected_keys = frozenset({"id", "headline", "created_at", "symbols"})

    def fetch(self, since, until):
        token = None
        while True:
            params = {
                "start": since.isoformat(),
                "end": until.isoformat(),
                "limit": 50,
                "sort": "asc",
                "symbols": ",".join(self.s.tickers),
                "include_content": "false",
            }
            if token:
                params["page_token"] = token
            res = get_json(
                self.http,
                "https://data.alpaca.markets/v1beta1/news",
                params=params,
                headers={"APCA-API-KEY-ID": self.s.alpaca_key, "APCA-API-SECRET-KEY": self.s.alpaca_secret},
            )
            yield from res.get("news", [])
            token = res.get("next_page_token")
            if not token:
                return

    def to_record(self, raw):
        return Record(self.name, str(raw["id"]), None, _dt(raw["created_at"]), _dt(raw.get("updated_at")), raw)


class EdgarForm8K(BaseSource):
    """8-K via l'API submissions (User-Agent déclaratif obligatoire ; <= 10 req/s)."""

    name = "sec_8k"
    expected_keys = frozenset({"accessionNumber", "form", "acceptanceDateTime"})

    def _cik_map(self) -> dict[str, str]:
        d = get_json(
            self.http,
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": self.s.sec_user_agent},
        )
        return {v["ticker"]: str(v["cik_str"]).zfill(10) for v in d.values()}

    def fetch(self, since, until):
        import time

        ciks = self._cik_map()
        for sym in self.s.tickers:
            cik = ciks.get(sym)
            if not cik:
                continue
            sub = get_json(
                self.http,
                f"https://data.sec.gov/submissions/CIK{cik}.json",
                headers={"User-Agent": self.s.sec_user_agent},
            )
            recent = sub["filings"]["recent"]
            for i, form in enumerate(recent["form"]):
                if form != "8-K":
                    continue
                row = {k: recent[k][i] for k in recent if len(recent[k]) > i}
                ts = _dt(row["acceptanceDateTime"])  # horodatage d'acceptation EDGAR (UTC)
                if ts and since <= ts <= until:
                    row.update(_symbol=sym, _cik=cik)
                    yield row
            time.sleep(0.15)  # ~6 req/s, sous la limite SEC

    def to_record(self, raw):
        return Record(
            self.name,
            f"{raw['_cik']}:{raw['accessionNumber']}",
            raw["_symbol"],
            _dt(raw["acceptanceDateTime"]),
            None,
            raw,
        )


class GdeltDoc(BaseSource):
    name = "gdelt"
    expected_keys = frozenset({"url", "title", "seendate"})
    QUERY = '(inflation OR "interest rates" OR Fed OR tariffs OR sanctions OR OPEC) sourcelang:eng'

    def fetch(self, since, until):
        res = get_json(
            self.http,
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={
                "query": self.QUERY,
                "mode": "artlist",
                "format": "json",
                "maxrecords": 250,
                "sort": "datedesc",
                "startdatetime": since.strftime("%Y%m%d%H%M%S"),
                "enddatetime": until.strftime("%Y%m%d%H%M%S"),
            },
        )
        yield from res.get("articles", [])

    def to_record(self, raw):
        return Record(
            self.name,
            raw["url"],
            None,
            _dt(pd.to_datetime(raw["seendate"], format="%Y%m%dT%H%M%SZ", utc=True)),
            None,
            raw,
        )


class RedditMemes(BaseSource):
    name = "reddit"
    expected_keys = frozenset({"id", "title", "created_utc"})
    SUBS = "wallstreetbets+stocks+investing+Superstonk"

    def fetch(self, since, until):
        import praw

        r = praw.Reddit(client_id=self.s.reddit_id, client_secret=self.s.reddit_secret, user_agent="nmp/0.1 by u/yourname")
        for p in r.subreddit(self.SUBS).new(limit=500):
            ts = datetime.fromtimestamp(p.created_utc, tz=timezone.utc)
            if since <= ts <= until:
                yield {
                    "id": p.id,
                    "title": p.title,
                    "selftext": p.selftext[:5000],
                    "score": p.score,
                    "num_comments": p.num_comments,
                    "created_utc": p.created_utc,
                    "subreddit": str(p.subreddit),
                }

    def to_record(self, raw):
        return Record(self.name, raw["id"], None, _dt(pd.to_datetime(raw["created_utc"], unit="s", utc=True)), None, raw)


class AlpacaBars1m(BaseSource):
    """Barres 1 min brutes (adjustment=raw) : on ajuste nous-mêmes -> recalculable et traçable."""

    name = "alpaca_bars_1m"
    bronze_table = "bronze.raw_bars"
    expected_keys = frozenset({"t", "o", "h", "l", "c", "v"})

    def fetch(self, since, until):
        for sym in self.s.bar_tickers:
            token = None
            while True:
                params = {
                    "symbols": sym,
                    "timeframe": "1Min",
                    "start": since.isoformat(),
                    "end": until.isoformat(),
                    "limit": 10000,
                    "adjustment": "raw",
                    "feed": self.s.alpaca_feed,
                }
                if token:
                    params["page_token"] = token
                res = get_json(
                    self.http,
                    "https://data.alpaca.markets/v2/stocks/bars",
                    params=params,
                    headers={
                        "APCA-API-KEY-ID": self.s.alpaca_key,
                        "APCA-API-SECRET-KEY": self.s.alpaca_secret,
                    },
                )
                for b in res.get("bars", {}).get(sym, []):
                    b["_symbol"] = sym
                    yield b
                token = res.get("next_page_token")
                if not token:
                    break

    def to_record(self, raw):
        return Record(self.name, f"{raw['_symbol']}:{raw['t']}", raw["_symbol"], _dt(raw["t"]), None, raw)


class YahooDaily(BaseSource):
    """Quotidien brut + actions corporatives (fractionnements, dividendes) + secteur."""

    name = "yahoo_daily"
    bronze_table = "bronze.raw_bars"
    expected_keys = frozenset({"Close"})

    def fetch(self, since, until):
        import yfinance as yf

        for sym in self.s.bar_tickers:
            h = yf.Ticker(sym).history(start=since.date(), end=until.date() + timedelta(days=1), auto_adjust=False, actions=True)
            for ts, row in h.iterrows():
                d = {k: (None if pd.isna(v) else float(v)) for k, v in row.items()}
                d.update(_symbol=sym, _ts=pd.Timestamp(ts).tz_convert("UTC").isoformat())
                yield d

    def to_record(self, raw):
        return Record(self.name, f"{raw['_symbol']}:{raw['_ts']}", raw["_symbol"], _dt(raw["_ts"]), None, raw)


REGISTRY = {
    c.name: c
    for c in (
        FinnhubNews,
        FinnhubSentiment,
        AlpacaNews,
        EdgarForm8K,
        GdeltDoc,
        RedditMemes,
        AlpacaBars1m,
        YahooDaily,
    )
}
