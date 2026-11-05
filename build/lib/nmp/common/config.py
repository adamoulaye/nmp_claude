"""Configuration centralisée : variables d'environnement ou secrets Databricks."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _secret(name: str, default: str = "") -> str:
    """Env d'abord ; sinon secret scope Databricks 'nmp' si disponible."""
    val = os.getenv(name)
    if val:
        return val
    try:  # pragma: no cover - uniquement sur Databricks
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        return DBUtils(SparkSession.builder.getOrCreate()).secrets.get("nmp", name)
    except Exception:
        return default


@dataclass(frozen=True)
class Settings:
    catalog: str = field(default_factory=lambda: os.getenv("NMP_CATALOG", "nmp_dev"))
    tickers: tuple[str, ...] = field(
        default_factory=lambda: tuple(t.strip() for t in os.getenv("TICKERS", "AAPL,MSFT").split(","))
    )
    benchmark: str = field(default_factory=lambda: os.getenv("BENCHMARK", "SPY"))
    finnhub_key: str = field(default_factory=lambda: _secret("FINNHUB_API_KEY"))
    alpaca_key: str = field(default_factory=lambda: _secret("ALPACA_KEY_ID"))
    alpaca_secret: str = field(default_factory=lambda: _secret("ALPACA_SECRET"))
    alpaca_feed: str = field(default_factory=lambda: os.getenv("ALPACA_FEED", "iex"))
    newsapi_key: str = field(default_factory=lambda: _secret("NEWSAPI_KEY"))
    reddit_id: str = field(default_factory=lambda: _secret("REDDIT_CLIENT_ID"))
    reddit_secret: str = field(default_factory=lambda: _secret("REDDIT_CLIENT_SECRET"))
    sec_user_agent: str = field(default_factory=lambda: os.getenv("SEC_USER_AGENT", "nmp contact@example.com"))
    kafka_bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    kafka_password: str = field(default_factory=lambda: _secret("KAFKA_SASL_PASSWORD"))
    overlap_minutes: int = 60  # chevauchement des fenêtres : sûr car l'ingestion est idempotente

    @property
    def bar_tickers(self) -> tuple[str, ...]:
        """Titres suivis + indice de référence (SPY) : le modèle de marché a besoin des deux séries."""
        return self.tickers if self.benchmark in self.tickers else (*self.tickers, self.benchmark)


def get_settings() -> Settings:
    return Settings()
