"""Mode local : de TOUTES les archives brutes (data/bronze) au CAR de chaque nouvelle en séance.

Chaîne : archives -> nouvelles normalisées -> regroupement des doublons (par titre) -> séance ('pendant') -> étude d'événements
(run_batch) -> tables Delta + CSV + synthèse. Rejouable à volonté : les tables de résultats sont réécrites (pas de doublons).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from math import nan, sqrt
from pathlib import Path
from typing import Any

import pandas as pd
from scipy import stats

from nmp.common.timeutil import attach_session, build_sessions
from nmp.local.schema import NEWS_COLS
from nmp.local.sources import alpaca, finnhub
from nmp.transform.car_batch import BENCHMARK, run_batch
from nmp.transform.category import categorize
from nmp.transform.dedup import cluster_articles
from nmp.transform.event_study import cross_sectional_test

KEEP = ("source", "publisher", "headline", "url", "cluster_size", "session_type")


def iter_raw(source: str, data_dir: Path) -> Iterator[tuple[str, object]]:
    """Lit les archives Bronze d'une source : data/bronze/<source>/<clé>/<jour>/<lot>.json -> (clé, contenu brut)."""
    for f in sorted((Path(data_dir) / "bronze" / source).glob("*/*/*.json")):
        rec = json.loads(f.read_text(encoding="utf-8"))
        yield rec["key"], rec["payload"]


def load_news(data_dir: Path, tickers: list[str]) -> pd.DataFrame:
    parts = [alpaca.normalize_news(p, tickers) for _, p in iter_raw("alpaca_news", data_dir)]  # type: ignore[arg-type]
    parts += [finnhub.normalize_news(p, k) for k, p in iter_raw("finnhub", data_dir) if k in tickers]  # type: ignore[arg-type]
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame(columns=NEWS_COLS)
    news = pd.concat(parts, ignore_index=True)
    # un même lot peut avoir été téléchargé plusieurs fois : une nouvelle = (article, titre)
    return news.drop_duplicates(["article_id", "ticker"]).reset_index(drop=True)


def load_bars(data_dir: Path) -> pd.DataFrame:
    pages = [pg for _, payload in iter_raw("alpaca_bars", data_dir) for pg in payload]  # type: ignore[attr-defined]
    return alpaca.normalize_bars(pages)


def select_events(news: pd.DataFrame, benchmark: str = BENCHMARK, threshold: float = 0.7) -> pd.DataFrame:
    """Premières publications (reprises exclues) publiées PENDANT la séance. Les titres sont comparés mot à mot (k=1)."""
    news = news[news["ticker"] != benchmark].copy()
    if news.empty:
        return news
    news["published_utc"] = pd.to_datetime(news["published_utc"], utc=True)
    news["text"] = news["headline"].fillna("")
    groups = [
        cluster_articles(g, id_col="article_id", url_col="url", threshold=threshold, k=1) for _, g in news.groupby("ticker")
    ]
    news = pd.concat(groups, ignore_index=True)
    lo, hi = news["published_utc"].min().date() - pd.Timedelta(days=7), news["published_utc"].max().date() + pd.Timedelta(days=7)
    news = attach_session(news, build_sessions(lo.isoformat(), hi.isoformat()))
    return news[news["is_first"] & (news["session_type"] == "pendant")].reset_index(drop=True)


def run(data_dir: Path, tickers: list[str], start=None, end=None, benchmark: str = BENCHMARK, return_inputs: bool = False):
    news, bars = load_news(data_dir, tickers), load_bars(data_dir)
    events = select_events(news, benchmark)
    if start is not None:
        events = events[events["published_utc"] >= pd.Timestamp(start, tz="UTC")]
    if end is not None:
        events = events[events["published_utc"] < pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)]
    stats = {"news_lues": len(news), "evenements_en_seance": len(events), "barres": len(bars)}
    if events.empty or bars.empty:
        out: tuple = (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), stats)
        return (*out, news, bars) if return_inputs else out
    res, windows, err = run_batch(events, bars, benchmark, keep=KEEP)
    if not res.empty:  # catégorie par règles (ligne de base explicable), sur le titre
        res["categorie"] = [categorize(h) for h in res["headline"].fillna("")]
    out = (res, windows, err, stats)
    return (*out, news, bars) if return_inputs else out


def summarize(res: pd.DataFrame) -> dict:
    """Synthèse : effet moyen, ampleur moyenne, part de résultats significatifs, fuites (événements écartés des tests)."""
    if res.empty:
        return {}
    clean = res[~res["leak_flag"].astype(bool)]
    out: dict[str, Any] = {"n_calcules": len(res), "n_fuite_ecartes": len(res) - len(clean), "n_testes": len(clean)}
    for w in ("car_0_15", "car_0_60"):
        p = "p_" + w[4:]
        if len(clean) >= 3:
            out[w] = cross_sectional_test(clean[w])
        out[w + "_ampleur_moyenne"] = clean[w].abs().mean() if len(clean) else float("nan")
        out[w + "_part_p<0.05"] = float((clean[p] < 0.05).mean()) if len(clean) else float("nan")
    return out


# ---------- Tests robustes : l'unité indépendante est (titre, jour), pas l'article ----------
def ticker_day_units(df: pd.DataFrame, col: str) -> pd.Series:
    """Moyenne du CAR par (titre, jour) : des articles du même jour partagent le même choc de marché, donc ne sont pas indépendants."""
    jour = pd.to_datetime(df["published_utc"], utc=True).dt.date
    return df.assign(jour=jour).groupby(["ticker", "jour"])[col].mean()


def effect(units: pd.Series) -> dict[str, float]:
    """Moyenne, IC à 95 %, p-values, et effet minimal détectable (puissance 80 % ≈ 2,8 erreurs standard)."""
    s = units.dropna()
    n = len(s)
    if n < 3:
        return {"n": n}
    mean, se = float(s.mean()), float(s.std(ddof=1)) / sqrt(n)
    half = float(stats.t.ppf(0.975, n - 1)) * se
    return {
        "n": n,
        "mean": mean,
        "ci_low": mean - half,
        "ci_high": mean + half,
        "p_ttest": float(stats.ttest_1samp(s, 0.0).pvalue),
        "p_wilcoxon": float(stats.wilcoxon(s).pvalue) if n > 5 else nan,
        "mde": 2.8 * se,
    }


def holm(pvals: list[float]) -> list[float]:
    """Correction de Holm (plusieurs catégories testées : sans correction, 1 sur 20 sort « significative » par hasard)."""
    m = len(pvals)
    order = sorted(range(m), key=lambda i: pvals[i])
    adj, running = [nan] * m, 0.0
    for rank, i in enumerate(order):
        running = max(running, min(1.0, (m - rank) * pvals[i]))
        adj[i] = running
    return adj


def robust_summary(res: pd.DataFrame, col: str = "car_0_60") -> dict[str, float]:
    return effect(ticker_day_units(res[~res["leak_flag"].astype(bool)], col))


def by_category(res: pd.DataFrame, col: str = "car_0_60", min_units: int = 5) -> pd.DataFrame:
    """Même test robuste, séparé par catégorie. Les catégories avec moins de `min_units` titres-jours ne sont pas testées."""
    clean = res[~res["leak_flag"].astype(bool)]
    rows = []
    for cat, g in clean.groupby("categorie"):
        e = effect(ticker_day_units(g, col))
        rows.append(
            {"categorie": cat, "evenements": len(g), "titres_jours": e["n"]}
            | {k: e.get(k, nan) for k in ("mean", "ci_low", "ci_high", "p_ttest", "mde")}
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    ok = out["titres_jours"] >= min_units
    out["p_holm"] = nan
    if ok.any():
        out.loc[ok, "p_holm"] = holm(out.loc[ok, "p_ttest"].tolist())
    return out.sort_values("evenements", ascending=False).reset_index(drop=True)
