"""Finnhub: actualites par entreprise (plan gratuit: environ 1 an d'historique, 60 appels/min)."""

import pandas as pd

from ..config import FINNHUB_API_KEY
from ..http_utils import make_session
from ..schema import NEWS_COLS

URL = "https://finnhub.io/api/v1/company-news"


def fetch_company_news(symbol: str, date_from: str, date_to: str, session=None) -> list[dict]:
    """date_from / date_to: 'YYYY-MM-DD'. Si une reponse semble plafonnee, reduire la fenetre."""
    s = session or make_session()
    r = s.get(URL, params={"symbol": symbol, "from": date_from, "to": date_to, "token": FINNHUB_API_KEY}, timeout=30)
    r.raise_for_status()
    return r.json()


def normalize_news(items: list[dict], ticker: str, ingested_utc=None) -> pd.DataFrame:
    ingested = ingested_utc or pd.Timestamp.now(tz="UTC")
    rows = [
        {
            "article_id": f"finnhub:{it['id']}",
            "source": "finnhub",
            "publisher": it.get("source"),
            "ticker": ticker,
            "published_utc": pd.to_datetime(it["datetime"], unit="s", utc=True),
            "updated_utc": pd.NaT,
            "ingested_utc": ingested,
            "headline": it.get("headline", ""),
            "summary": it.get("summary", ""),
            "url": it.get("url", ""),
        }
        for it in items
        if it.get("datetime")
    ]
    return pd.DataFrame(rows, columns=NEWS_COLS)
