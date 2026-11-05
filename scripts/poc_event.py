"""Semaine 1: preuve de concept de BOUT EN BOUT sur une nouvelle (article -> prix -> CAR -> Delta).
Usage: python scripts/poc_event.py --ticker AAPL --day 2026-10-13 [--finbert]"""

import argparse
from datetime import date, timedelta

import matplotlib.pyplot as plt
import pandas as pd

from nmp.local.config import BENCHMARK, CODE_VERSION
from nmp.transform.dedup import cluster_articles
from nmp.local.delta_io import to_delta
from nmp.transform.event_study import compute_event
from nmp.common.timeutil import classify_session
from nmp.local.sources import alpaca, finnhub


def main(ticker: str, day: date, use_finbert: bool, pick: int):
    d0, d1 = day.isoformat(), (day + timedelta(days=1)).isoformat()
    # 1. Nouvelles de deux sources, normalisees puis regroupees (premiere publication = signal)
    news = pd.concat(
        [
            alpaca.normalize_news(alpaca.fetch_news([ticker], f"{d0}T00:00:00Z", f"{d1}T00:00:00Z"), [ticker]),
            finnhub.normalize_news(finnhub.fetch_company_news(ticker, d0, d0), ticker),
        ]
    )
    news["text"] = news["headline"].fillna("")
    news = cluster_articles(news, id_col="article_id", url_col="url", threshold=0.7, k=1)  # titres : mots
    news[["session", "reaction_start_utc"]] = [classify_session(t) for t in news["published_utc"]]
    cand = news[news["is_first"] & (news["session"] == "pendant")].reset_index(drop=True)
    if cand.empty:
        raise SystemExit("Aucune nouvelle en seance ce jour-la: essayez un autre jour.")
    art = cand.iloc[pick]
    print(f"Article {art.article_id} | {art.published_utc} | {art.headline[:80]}")

    # 2. Prix minute (5 jours avant pour l'estimation, +61 min apres)
    pub = art.published_utc
    start = (pub - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (pub + timedelta(minutes=61)).strftime("%Y-%m-%dT%H:%M:%SZ")
    bars = alpaca.normalize_bars(alpaca.fetch_bars([ticker, BENCHMARK], start, end))
    bs, bm = bars[bars.ticker == ticker], bars[bars.ticker == BENCHMARK]

    # 3. Sentiment (optionnel) et rendement anormal
    sent = {}
    if use_finbert:
        from nmp.ml.sentiment import FinBertScorer

        sent = FinBertScorer().score([f"{art.headline}. {art.summary}"]).iloc[0].to_dict()
    summary, window = compute_event(bs, bm, pub)
    print({k: round(v, 5) if isinstance(v, float) else v for k, v in summary.items()})

    # 4. Graphique prix / volume avec l'instant de publication
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(9, 5))
    lo, hi = summary["m0_utc"] - timedelta(minutes=15), summary["m0_utc"] + timedelta(minutes=60)
    w = bs[(bs.ts_utc >= lo) & (bs.ts_utc <= hi)]
    ax1.plot(w.ts_utc, w.close)
    ax2.bar(w.ts_utc, w.volume, width=0.0006)
    for ax in (ax1, ax2):
        ax.axvline(summary["m0_utc"], color="red", ls="--")
    ax1.set_title(f"{ticker} - {art.headline[:70]}")
    fig.savefig(f"poc_{ticker}_{d0}.png", dpi=120)

    # 5. Tables Delta: resume (1 ligne) + fenetre detaillee (traçabilite)
    key = {"article_id": art.article_id, "ticker": ticker}
    res = pd.DataFrame(
        [
            {
                **key,
                "source": art.source,
                "published_utc": pub,
                "session": art.session,
                "headline": art.headline,
                **sent,
                **summary,
                "code_version": CODE_VERSION,
                "computed_at": pd.Timestamp.now(tz="UTC"),
            }
        ]
    )
    to_delta(res, "event_results")
    to_delta(window.assign(**key), "event_windows")
    print("Ecrit dans data/delta/event_results et data/delta/event_windows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", default="AAPL")
    ap.add_argument("--day", type=date.fromisoformat, required=True)
    ap.add_argument("--finbert", action="store_true")
    ap.add_argument("--pick", type=int, default=0, help="n-ieme nouvelle de la journee")
    a = ap.parse_args()
    main(a.ticker, a.day, a.finbert, a.pick)
