"""Construction de Silver Yahoo Finance à partir des archives Bronze."""

import json

import numpy as np
import pandas as pd

from nmp.local.config import DATA_DIR, PROJECT_TICKERS, BENCHMARK
from nmp.local.delta_io import to_delta


def latest_archives(source: str) -> dict:
    """Récupère la dernière archive disponible pour chaque symbole."""
    folder = DATA_DIR / "bronze" / source
    latest = {}

    for path in folder.glob("*/*/*.json"):
        with path.open(encoding="utf-8") as f:
            archive = json.load(f)

        symbol = archive["key"]
        timestamp = archive.get("ingested_at", "")

        if symbol not in latest or timestamp > latest[symbol][0]:
            latest[symbol] = (timestamp, archive)

    return {
        symbol: archive
        for symbol, (_, archive) in latest.items()
    }


def load_daily_prices() -> pd.DataFrame:
    """Charge les cours quotidiens depuis Bronze."""
    archives = latest_archives("yahoo_daily")
    records = []

    for _symbol, archive in archives.items():
        payload = archive.get("payload") or []

        if isinstance(payload, list):
            records.extend(payload)

    if not records:
        raise RuntimeError("Aucune donnée quotidienne Yahoo trouvée dans Bronze.")

    df = pd.DataFrame(records)

    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
        "Dividends": "dividends",
        "Stock Splits": "stock_splits",
    })

    required = [
        "symbol", "date", "open", "high", "low",
        "close", "adj_close", "volume", "dividends", "stock_splits",
    ]

    for column in required:
        if column not in df.columns:
            df[column] = None

    df = df[required].copy()

    df["date"] = pd.to_datetime(
        df["date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    numeric_cols = [
        "open", "high", "low", "close", "adj_close",
        "volume", "dividends", "stock_splits",
    ]

    for column in numeric_cols:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=["symbol", "date", "close"])

    # Une seule observation par titre et par date.
    df = (
        df.sort_values(["symbol", "date"])
        .drop_duplicates(["symbol", "date"], keep="last")
        .reset_index(drop=True)
    )

    return df


def load_reference() -> pd.DataFrame:
    """Charge les métadonnées sectorielles Yahoo Finance."""
    archives = latest_archives("yahoo_reference")
    records = []

    for _symbol, archive in archives.items():
        profile = archive.get("payload") or {}

        if isinstance(profile, dict):
            records.append(profile)

    columns = [
        "symbol", "name", "sector",
        "industry", "currency", "exchange",
    ]

    if not records:
        return pd.DataFrame(columns=columns)

    reference = pd.DataFrame(records)

    for column in columns:
        if column not in reference.columns:
            reference[column] = None

    return reference[columns].drop_duplicates("symbol", keep="last")


def build_silver() -> pd.DataFrame:
    """Construit la table Silver quotidienne."""
    prices = load_daily_prices()
    reference = load_reference()

    df = prices.merge(
        reference,
        on="symbol",
        how="left",
        validate="many_to_one",
    )

    df["is_benchmark"] = df["symbol"].eq(BENCHMARK)

    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    # Rendements calculés à partir des cours de clôture non ajustés.
    previous_close = df.groupby("symbol")["close"].shift(1)

    df["return_close"] = (
        df["close"] / previous_close - 1
    )

    df["log_return_close"] = np.log(
        df["close"] / previous_close
    )

    previous_adj_close = df.groupby("symbol")["adj_close"].shift(1)

    df["return_adj_close"] = (
        df["adj_close"] / previous_adj_close - 1
    )

    df["log_return_adj_close"] = np.log(
        df["adj_close"] / previous_adj_close
    )

    # Volume monétaire quotidien approximatif.
    df["dollar_volume"] = df["close"] * df["volume"]

    df["year"] = pd.to_datetime(df["date"]).dt.year

    columns = [
        "symbol", "date", "year",
        "name", "sector", "industry",
        "currency", "exchange", "is_benchmark",
        "open", "high", "low", "close", "adj_close", "volume",
        "dividends", "stock_splits",
        "return_close", "log_return_close",
        "return_adj_close", "log_return_adj_close",
        "dollar_volume",
    ]

    return df[columns]


def main():
    print("=" * 65)
    print("CONSTRUCTION SILVER - YAHOO FINANCE")
    print("=" * 65)

    df = build_silver()

    expected_symbols = set(PROJECT_TICKERS) | {BENCHMARK}
    actual_symbols = set(df["symbol"].unique())

    unexpected = actual_symbols - expected_symbols
    missing = expected_symbols - actual_symbols

    if unexpected:
        raise ValueError(f"Symboles inattendus : {sorted(unexpected)}")

    if missing:
        raise ValueError(f"Symboles absents : {sorted(missing)}")

    if df.duplicated(["symbol", "date"]).any():
        raise ValueError("Doublons détectés sur symbol-date.")

    # Écriture dans la table Delta locale.
    delta_path = to_delta(
        df,
        name="yahoo_daily",
        mode="overwrite",
    )

    # Export Parquet pour les analyses locales.
    parquet_path = DATA_DIR / "silver" / "yahoo_daily" / "market_daily.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False, compression="snappy")

    print("\nCONSTRUCTION TERMINÉE")
    print("-" * 65)
    print(f"Observations       : {len(df):,}")
    print(f"Titres             : {df['symbol'].nunique()}")
    print(f"Première date      : {df['date'].min()}")
    print(f"Dernière date      : {df['date'].max()}")
    print(f"Doublons           : {df.duplicated(['symbol', 'date']).sum()}")
    print(f"Secteurs identifiés: {df['sector'].nunique()}")
    print(f"Benchmark inclus   : {df['is_benchmark'].any()}")
    print(f"\nDelta              : {delta_path}")
    print(f"Parquet            : {parquet_path}")

    print("\nOBSERVATIONS PAR TITRE")
    print(df.groupby("symbol").size().to_string())

    print("\nSECTEURS")
    print(
        df[["symbol", "sector"]]
        .drop_duplicates()
        .sort_values("symbol")
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
