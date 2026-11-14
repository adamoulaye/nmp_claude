"""Connecteur Yahoo Finance pour les données quotidiennes locales."""

from datetime import timedelta

import pandas as pd
import yfinance as yf


def fetch_daily(symbol: str, start, end) -> list[dict]:
    """Télécharge OHLCV, cours ajustés et actions corporatives."""
    ticker = yf.Ticker(symbol)

    history = ticker.history(
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),
        auto_adjust=False,
        actions=True,
    )

    records = []

    for timestamp, row in history.iterrows():
        record = {
            "symbol": symbol,
            "date": timestamp.date().isoformat(),
        }

        for column in (
            "Open",
            "High",
            "Low",
            "Close",
            "Adj Close",
            "Volume",
            "Dividends",
            "Stock Splits",
        ):
            value = row.get(column)
            record[column] = None if pd.isna(value) else float(value)

        records.append(record)

    return records


def fetch_profile(symbol: str) -> dict:
    """Récupère les métadonnées sectorielles de l'entreprise."""
    info = yf.Ticker(symbol).get_info()

    return {
        "symbol": symbol,
        "name": info.get("longName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "currency": info.get("currency"),
        "exchange": info.get("exchange"),
    }
