"""SEC EDGAR: formulaires 8-K. La SEC EXIGE un User-Agent avec vos coordonnees; max 10 req/s."""

import time

import pandas as pd

from ..config import SEC_USER_AGENT
from ..http_utils import make_session


def _session():
    if not SEC_USER_AGENT:
        raise RuntimeError("Definir SEC_USER_AGENT dans .env (ex. 'Prenom Nom email@univ.ca')")
    return make_session({"User-Agent": SEC_USER_AGENT})


def ticker_to_cik(session=None) -> dict[str, str]:
    s = session or _session()
    r = s.get("https://www.sec.gov/files/company_tickers.json", timeout=30)
    r.raise_for_status()
    return {v["ticker"]: str(v["cik_str"]).zfill(10) for v in r.json().values()}


def fetch_submissions(cik10: str, session=None) -> dict:
    s = session or _session()
    time.sleep(0.15)  # respecter la limite de debit
    r = s.get(f"https://data.sec.gov/submissions/CIK{cik10}.json", timeout=30)
    r.raise_for_status()
    return r.json()


def normalize_8k(sub: dict, ticker: str) -> pd.DataFrame:
    rec = pd.DataFrame(sub["filings"]["recent"])
    rec = rec[rec["form"] == "8-K"].copy()
    cik = int(sub["cik"])
    rec["article_id"] = "sec:" + rec["accessionNumber"]
    rec["source"], rec["publisher"], rec["ticker"] = "sec", "SEC EDGAR", ticker
    # acceptanceDateTime = instant exact de reception par la SEC (plus precis que filingDate)
    rec["published_utc"] = pd.to_datetime(rec["acceptanceDateTime"], utc=True)
    rec["updated_utc"] = pd.NaT
    rec["ingested_utc"] = pd.Timestamp.now(tz="UTC")
    rec["headline"] = "8-K items: " + rec["items"].fillna("")
    rec["summary"] = ""
    rec["url"] = (
        "https://www.sec.gov/Archives/edgar/data/"
        + str(cik)
        + "/"
        + rec["accessionNumber"].str.replace("-", "", regex=False)
        + "/"
        + rec["primaryDocument"]
    )
    return rec[
        [
            "article_id",
            "source",
            "publisher",
            "ticker",
            "published_utc",
            "updated_utc",
            "ingested_utc",
            "headline",
            "summary",
            "url",
        ]
    ].reset_index(drop=True)
