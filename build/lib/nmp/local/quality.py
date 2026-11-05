"""SLO, controles de fraicheur et alertes (source muette, taux de succes des pipelines)."""

import os
from typing import Any

import pandas as pd
import requests

from nmp.common.timeutil import classify_session

from .config import DATA_DIR

SLO: dict[str, Any] = {
    "max_age_min": {"finnhub": 10, "alpaca_news": 5, "alpaca_bars": 3, "sec_8k": 60},
    "pipeline_success_rate": 0.99,
}


def load_runs(path=None) -> pd.DataFrame:
    runs = pd.read_json(path or DATA_DIR / "logs" / "runs.jsonl", lines=True)
    runs["started_at"] = pd.to_datetime(runs["started_at"], utc=True)
    return runs


def pipeline_success_rate(runs: pd.DataFrame, hours: int = 24, now=None) -> float:
    now = now or pd.Timestamp.now(tz="UTC")
    recent = runs[runs["started_at"] >= now - pd.Timedelta(hours=hours)]
    return float((recent["status"] == "ok").mean()) if len(recent) else float("nan")


def silent_sources(runs: pd.DataFrame, now=None, ignore_market_hours: bool = False) -> list[dict]:
    """Une source est MUETTE si aucune execution reussie recente alors que le marche est ouvert.
    ignore_market_hours=True: utile pour une demonstration hors des heures de bourse."""
    now = now or pd.Timestamp.now(tz="UTC")
    is_open = ignore_market_hours or classify_session(now)[0] == "pendant"
    ok = runs[runs["status"] == "ok"].groupby("source")["started_at"].max()
    alerts = []
    for source, limit in SLO["max_age_min"].items():
        last = ok.get(source)
        age = (now - last).total_seconds() / 60 if last is not None else float("inf")
        if is_open and age > limit:
            alerts.append({"source": source, "age_min": round(age, 1), "limit_min": limit})
    return alerts


def send_alert(message: str) -> None:
    print(f"[ALERTE] {message}")
    url = os.getenv("ALERT_WEBHOOK_URL")  # webhook entrant Teams ou Slack
    if url:
        requests.post(url, json={"text": message}, timeout=10)


def check_bars(bars: pd.DataFrame) -> dict[str, int]:
    """Controles de qualite des barres a CHAQUE chargement. Retourne le nb de violations par regle."""
    o, h, lo, c = bars["open"], bars["high"], bars["low"], bars["close"]
    return {
        "doublons_ticker_minute": int(bars.duplicated(["ticker", "ts_utc"]).sum()),
        "prix_non_positifs": int((bars[["open", "high", "low", "close"]] <= 0).any(axis=1).sum()),
        "volume_negatif": int((bars["volume"] < 0).sum()),
        "ohlc_incoherent": int(
            ((h < pd.concat([o, c], axis=1).max(axis=1)) | (lo > pd.concat([o, c], axis=1).min(axis=1))).sum()
        ),
        "horodatage_non_utc": 0 if str(bars["ts_utc"].dtype).endswith("UTC]") else len(bars),
    }


def text_price_desync(last_news_ingested, last_bar_ts, max_minutes: float = 10.0, now=None) -> bool:
    """True = ALERTE: pendant la seance, texte et prix n'avancent plus ensemble (une des deux
    chaines est en retard de plus de `max_minutes` sur l'autre ou sur l'horloge)."""
    now = now or pd.Timestamp.now(tz="UTC")
    if classify_session(now)[0] != "pendant":
        return False
    gap = abs((last_news_ingested - last_bar_ts).total_seconds()) / 60
    stale = max((now - last_news_ingested).total_seconds(), (now - last_bar_ts).total_seconds()) / 60
    return gap > max_minutes or stale > 2 * max_minutes
