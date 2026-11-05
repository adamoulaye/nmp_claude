"""Jour 1: teste chaque source et mesure l'HISTORIQUE reellement accessible avec vos cles.
Usage: python scripts/check_apis.py"""

from datetime import date, timedelta

from nmp.local.sources import alpaca, finnhub, sec

TODAY = date.today()


def attempt(label, fn):
    try:
        print(f"[OK]     {label}: {fn()}")
    except Exception as e:  # noqa: BLE001  (on veut voir l erreur, pas planter)
        print(f"[ECHEC]  {label}: {type(e).__name__}: {str(e)[:150]}")


def finnhub_recent():
    items = finnhub.fetch_company_news("AAPL", str(TODAY - timedelta(days=3)), str(TODAY))
    return f"{len(items)} articles sur 3 jours"


def finnhub_old():
    d = TODAY - timedelta(days=360)
    items = finnhub.fetch_company_news("AAPL", str(d), str(d + timedelta(days=7)))
    return f"{len(items)} articles il y a ~360 jours"


def alpaca_news_old():
    pages = alpaca.fetch_news(["AAPL"], "2021-03-01T00:00:00Z", "2021-03-02T00:00:00Z")
    return f"{len(alpaca.normalize_news(pages))} lignes de nouvelles (mars 2021)"


def alpaca_bars(feed, start, end):
    def run():
        pages = alpaca.fetch_bars(["AAPL", "SPY"], start, end, feed=feed)
        df = alpaca.normalize_bars(pages)
        return f"{len(df)} barres ({feed})"

    return run


def sec_8k():
    s = sec._session()
    cik = sec.ticker_to_cik(s)["AAPL"]
    df = sec.normalize_8k(sec.fetch_submissions(cik, s), "AAPL")
    return f"{len(df)} formulaires 8-K recents, dernier: {df['published_utc'].max()}"


if __name__ == "__main__":
    attempt("Finnhub recent", finnhub_recent)
    attempt("Finnhub ~1 an", finnhub_old)
    attempt("Alpaca news 2021", alpaca_news_old)
    a, b = "2024-03-04T14:30:00Z", "2024-03-04T15:30:00Z"
    attempt("Alpaca barres IEX (2024)", alpaca_bars("iex", a, b))
    attempt("Alpaca barres SIP (2024)", alpaca_bars("sip", a, b))
    attempt("SEC EDGAR 8-K", sec_8k)
    print("\nNotez dans docs/sources.md: quotas, historique, volumes IEX vs SIP, conditions d'usage.")
