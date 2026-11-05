"""Deux questions de plus, posées aux CAR déjà calculés :

1. SENTIMENT — le SENS de la nouvelle prédit-il le SENS du mouvement ? (sans le signe, positifs et négatifs s'annulent)
2. TEST DE CONTRÔLE (placebo) — le cours bouge-t-il PLUS autour des nouvelles qu'à des moments comparables SANS nouvelle ?
   Les moments témoins sont tirés aux mêmes heures de la journée (la volatilité n'est pas constante), les jours sans l'événement,
   hors de toute fenêtre contenant une nouvelle du même titre.

Dans les deux cas l'unité statistique indépendante reste le (titre, jour) : voir events_batch.ticker_day_units.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from nmp.local.events_batch import effect, ticker_day_units
from nmp.transform.event_study import compute_event

# ---------------------------------------------------------------- 1. sentiment
POS = set(
    """beat beats beating surge surges surged soar soars soared jump jumps jumped rally rallies rallied rise rises rose gain gains
gained upgrade upgrades upgraded raises raised record strong tops topped outperform outperforms boost boosts growth approval approved
bullish overweight climb climbs win wins positive""".split()
)
NEG = set(
    """miss misses missed plunge plunges plunged tumble tumbles tumbled fall falls fell drop drops dropped slump slumps sink sinks
downgrade downgrades downgraded cut cuts lower lowers lowered lawsuit sues sued probe investigation recall recalls warns warning weak
worst retreat retreats slides slid loss losses decline declines bearish underweight fraud halt halts negative""".split()
)


def lexicon_scorer(texts: list[str]) -> pd.DataFrame:
    """LIGNE DE BASE grossière (liste de mots maison, sans contexte) : sert quand FinBERT n'est pas installé. Même format que FinBertScorer."""
    rows = []
    for t in texts:
        toks = re.findall(r"[a-z']+", (t or "").lower())
        p, n = sum(w in POS for w in toks), sum(w in NEG for w in toks)
        z = p + n + 1.0
        label = "positive" if p > n else "negative" if n > p else "neutral"
        rows.append(
            {"positive": p / z, "negative": n / z, "neutral": 1 / z, "sentiment_score": (p - n) / z, "sentiment_label": label}
        )
    return pd.DataFrame(rows, columns=["positive", "negative", "neutral", "sentiment_score", "sentiment_label"])


def add_sentiment(
    res: pd.DataFrame, method: str = "lexique", scorer: Callable[[list[str]], pd.DataFrame] | None = None
) -> pd.DataFrame:
    """Ajoute sentiment_score (−1…+1) et sentiment_label au tableau des événements. method : 'aucun' | 'lexique' | 'finbert'."""
    if method == "aucun" or res.empty:
        return res
    if scorer is None:
        if method == "finbert":
            from nmp.ml.sentiment import FinBertScorer  # exige l'extra [ml] (torch, transformers) et le téléchargement du modèle

            scorer = FinBertScorer().score
        else:
            scorer = lexicon_scorer
    s = scorer(res["headline"].fillna("").tolist())
    out = res.copy()
    for c in ("positive", "negative", "neutral", "sentiment_score", "sentiment_label"):
        out[c] = s[c].to_numpy()
    out["sentiment_methode"] = method
    return out


def sentiment_tests(res: pd.DataFrame, col: str = "car_0_60") -> dict[str, Any]:
    """Effet moyen par étiquette, CAR « signé » (positif : +CAR, négatif : −CAR ; > 0 si le sens est prédit) et corrélation de rang."""
    d = res[~res["leak_flag"].astype(bool)].copy()
    out: dict[str, Any] = {"repartition": d["sentiment_label"].value_counts().to_dict(), "par_etiquette": {}}
    for lab, g in d.groupby("sentiment_label"):
        out["par_etiquette"][lab] = {"evenements": len(g)} | effect(ticker_day_units(g, col))
    nn = d[d["sentiment_label"] != "neutral"].copy()
    nn["signe"] = nn[col] * np.where(nn["sentiment_label"] == "positive", 1.0, -1.0)
    out["signe"] = effect(ticker_day_units(nn, "signe")) if len(nn) else {"n": 0}
    g = (
        d.assign(jour=pd.to_datetime(d["published_utc"], utc=True).dt.date)
        .groupby(["ticker", "jour"])
        .agg(s=("sentiment_score", "mean"), c=(col, "mean"))
    )
    if len(g) >= 5 and g["s"].nunique() > 1:
        rho, p = stats.spearmanr(g["s"], g["c"])
        out["spearman"] = {"n": len(g), "rho": float(rho), "p": float(p)}
    return out


# ---------------------------------------------------------------- 2. placebo
ET = "America/New_York"
MIN = pd.Timedelta(minutes=1)


def draw_placebo_times(
    event_utc: pd.Timestamp,
    bars_ts: pd.DatetimeIndex,
    news_ns: np.ndarray,
    k: int,
    rng: np.random.Generator,
    tod_window: int = 30,
) -> list[pd.Timestamp]:
    """k instants témoins : même titre, autre jour, heure (New York) à ± tod_window min de l'événement, sans nouvelle dans [t−15, t+60] min."""
    et = bars_ts.tz_convert(ET)
    mod = (et.hour * 60 + et.minute).to_numpy()
    day = (et.year * 10000 + et.month * 100 + et.day).to_numpy()
    e = pd.Timestamp(event_utc).tz_convert(ET)
    mask = (np.abs(mod - (e.hour * 60 + e.minute)) <= tod_window) & (day != e.year * 10000 + e.month * 100 + e.day)
    cand = bars_ts[mask]
    if len(news_ns):
        lo = np.searchsorted(news_ns, (cand - 15 * MIN).to_numpy(), side="left")
        hi = np.searchsorted(news_ns, (cand + 60 * MIN).to_numpy(), side="right")
        cand = cand[lo == hi]
    if len(cand) == 0:
        return []
    pick = rng.choice(len(cand), size=min(k, len(cand)), replace=False)
    return [cand[i] for i in sorted(pick)]


def run_placebo(
    res: pd.DataFrame, bars: pd.DataFrame, news_all: pd.DataFrame, benchmark: str = "SPY", k: int = 10, seed: int = 7
) -> pd.DataFrame:
    """Pour chaque événement de `res` : k CAR « témoins ». Retourne une ligne par témoin (event_idx = index de l'événement dans res)."""
    rng = np.random.default_rng(seed)
    by = {t: g.sort_values("ts_utc") for t, g in bars.groupby("ticker")}
    news_ns = {t: np.sort(pd.to_datetime(g["published_utc"], utc=True).to_numpy()) for t, g in news_all.groupby("ticker")}
    ts_idx = {t: pd.DatetimeIndex(g["ts_utc"]) for t, g in by.items()}
    rows = []
    for idx, ev in res.iterrows():
        t = ev["ticker"]
        if t not in by:
            continue
        for ts in draw_placebo_times(
            ev["published_utc"], ts_idx[t], news_ns.get(t, np.array([], dtype="datetime64[ns]")), k, rng
        ):
            try:
                s, _ = compute_event(by[t], by[benchmark], ts)
            except (KeyError, ValueError):
                continue
            rows.append(
                {"event_idx": idx, "ticker": t, "placebo_utc": ts, **{c: s[c] for c in ("car_0_15", "car_0_60", "leak_flag")}}
            )
    return pd.DataFrame(rows, columns=["event_idx", "ticker", "placebo_utc", "car_0_15", "car_0_60", "leak_flag"])


def compare_to_placebo(res: pd.DataFrame, plc: pd.DataFrame, col: str = "car_0_60", min_placebos: int = 3) -> dict[str, float]:
    """Écart d'AMPLEUR |CAR| réel − |CAR| témoin moyen, par événement, puis moyenné par (titre, jour). > 0 : plus de mouvement autour des nouvelles."""
    real = res[~res["leak_flag"].astype(bool)]
    p = plc[~plc["leak_flag"].astype(bool)].assign(amp=lambda x: x[col].abs())
    m = p.groupby("event_idx").agg(amp_placebo=("amp", "mean"), n_placebo=("amp", "size"))
    j = real.assign(amp=real[col].abs()).join(m, how="inner")
    j = j[j["n_placebo"] >= min_placebos]
    if j.empty:
        return {"n_evenements": 0}
    j["ecart"] = j["amp"] - j["amp_placebo"]
    return {
        "n_evenements": len(j),
        "amplitude_reelle": float(j["amp"].mean()),
        "amplitude_placebo": float(j["amp_placebo"].mean()),
        "ratio": float(j["amp"].mean() / j["amp_placebo"].mean()),
    } | {f"ecart_{k}": v for k, v in effect(ticker_day_units(j, "ecart")).items()}
