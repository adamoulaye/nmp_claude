
"""Configuration centralisée : variables d'environnement ou secrets Databricks."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()

DEFAULT_TICKERS = (
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META",
    "ORCL", "AVGO", "AMD", "QCOM", "MU", "INTC",
    "DELL", "IBM", "CSCO", "PANW", "CRM", "SHOP",
    "TSLA", "UBER", "MCD",
    "GSK", "ZTS",
    "MMM", "ETN", "PWR",
    "BX", "APO",
)


def _secret(name: str, default: str = "") -> str:
    """Env d'abord ; sinon secret scope Databricks 'nmp' si disponible."""
    val = os.getenv(name)
    if val:
        return val

    try:  # pragma: no cover - uniquement sur Databricks
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        return DBUtils(
            SparkSession.builder.getOrCreate()
        ).secrets.get("nmp", name)

    except Exception:
        return default


def _tickers() -> tuple[str, ...]:
    """Lit les titres suivis depuis TICKERS ou utilise l'univers par défaut."""
    raw = os.getenv("TICKERS", ",".join(DEFAULT_TICKERS))

    values = tuple(
        dict.fromkeys(
            ticker.strip().upper()
            for ticker in raw.split(",")
            if ticker.strip()
        )
    )

    if not values:
        raise ValueError("TICKERS doit contenir au moins un symbole.")

    return values


@dataclass(frozen=True)
class Settings:
    catalog: str = field(
        default_factory=lambda: os.getenv("NMP_CATALOG", "mnp_dev")
    )

    tickers: tuple[str, ...] = field(default_factory=_tickers)

    benchmark: str = field(
        default_factory=lambda: os.getenv("BENCHMARK", "SPY").strip().upper()
    )

    finnhub_key: str = field(
        default_factory=lambda: _secret("FINNHUB_API_KEY")
    )

    alpaca_key: str = field(
        default_factory=lambda: _secret("ALPACA_KEY_ID")
    )

    alpaca_secret: str = field(
        default_factory=lambda: _secret("ALPACA_SECRET")
    )

    # Flux temps réel.
    alpaca_feed: str = field(
        default_factory=lambda: os.getenv("ALPACA_FEED", "iex").lower()
    )

    # Flux utilisé pour les données historiques.
    # IEX est le choix prudent par défaut.
    # Utiliser SIP uniquement si les droits du compte le permettent.
    alpaca_historical_feed: str = field(
        default_factory=lambda: os.getenv(
            "ALPACA_HISTORICAL_FEED", "iex"
        ).lower()
    )

    newsapi_key: str = field(
        default_factory=lambda: _secret("NEWSAPI_KEY")
    )

    reddit_id: str = field(
        default_factory=lambda: _secret("REDDIT_CLIENT_ID")
    )

    reddit_secret: str = field(
        default_factory=lambda: _secret("REDDIT_CLIENT_SECRET")
    )

    sec_user_agent: str = field(
        default_factory=lambda: os.getenv(
            "SEC_USER_AGENT", "NMP Research adamlaye06@gmail.com"
        )
    )

    kafka_bootstrap: str = field(
        default_factory=lambda: os.getenv(
            "KAFKA_BOOTSTRAP", "nmp-ns-dev-2026.servicebus.windows.net:9093"
        )
    )

    kafka_password: str = field(
        default_factory=lambda: _secret("KAFKA_SASL_PASSWORD")
    )

    overlap_minutes: int = 60

    @property
    def bar_tickers(self) -> tuple[str, ...]:
        """Actions suivies et indice de référence SPY."""
        if self.benchmark in self.tickers:
            return self.tickers

        return (*self.tickers, self.benchmark)


def get_settings() -> Settings:
    return Settings()
