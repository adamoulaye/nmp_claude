"""Étude d'événements quotidienne avec Yahoo Finance et SPY."""

import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import t as student_t


DATA_DIR = Path("data")
PRICES_FILE = DATA_DIR / "silver/yahoo_daily/market_daily.parquet"
EVENTS_FILE = DATA_DIR / "exports/event_results_batch.csv"

OUTPUT_DIR = DATA_DIR / "exports/daily_event_study"

ESTIMATION_START = -120
ESTIMATION_END = -21
MIN_ESTIMATION_OBS = 60

EVENT_START = -5
EVENT_END = 5

BENCHMARK = "SPY"


def load_prices():
    """Charge les rendements ajustés des actions et du benchmark."""
    df = pd.read_parquet(PRICES_FILE)

    df["date"] = pd.to_datetime(df["date"])

    df = df[
        ["symbol", "date", "return_adj_close"]
    ].copy()

    df["return_adj_close"] = pd.to_numeric(
        df["return_adj_close"], errors="coerce"
    )

    prices = df.pivot(
        index="date",
        columns="symbol",
        values="return_adj_close",
    ).sort_index()

    if BENCHMARK not in prices.columns:
        raise ValueError(f"Benchmark {BENCHMARK} absent des données.")

    return prices


def load_events():
    """Charge les événements financiers déjà enrichis."""
    events = pd.read_csv(EVENTS_FILE)

    required = ["article_id", "ticker", "published_utc"]

    missing = set(required) - set(events.columns)

    if missing:
        raise ValueError(f"Colonnes absentes des événements : {missing}")

    events["published_utc"] = pd.to_datetime(
        events["published_utc"], utc=True, errors="coerce"
    )

    events = events.dropna(
        subset=["article_id", "ticker", "published_utc"]
    ).copy()

    return events


def map_event_date(timestamp, trading_dates):
    """Affecte une publication à sa séance boursière."""
    ny_time = timestamp.tz_convert("America/New_York")

    event_date = np.datetime64(ny_time.date())

    # Après la clôture : prochaine séance.
    after_close = (
        ny_time.hour > 16
        or (ny_time.hour == 16 and ny_time.minute >= 0)
    )

    idx = np.searchsorted(trading_dates, event_date, side="left")

    if idx >= len(trading_dates):
        return None

    if trading_dates[idx] == event_date and after_close:
        idx += 1

    if idx >= len(trading_dates):
        return None

    return pd.Timestamp(trading_dates[idx])


def fit_market_model(stock_returns, market_returns):
    """Estime R_i = alpha + beta * R_SPY."""
    valid = (
        stock_returns.notna()
        & market_returns.notna()
    )

    y = stock_returns[valid].to_numpy(dtype=float)
    x = market_returns[valid].to_numpy(dtype=float)

    if len(y) < MIN_ESTIMATION_OBS:
        return None

    X = np.column_stack([np.ones(len(x)), x])

    coefficients, _, _, _ = np.linalg.lstsq(X, y, rcond=None)

    alpha, beta = coefficients

    residuals = y - X @ coefficients
    sigma = np.sqrt(np.sum(residuals ** 2) / (len(y) - 2))

    if not np.isfinite(sigma) or sigma <= 0:
        return None

    sxx = np.sum((x - x.mean()) ** 2)

    if sxx <= 0:
        return None

    r2 = 1 - (
        np.sum(residuals ** 2)
        / np.sum((y - y.mean()) ** 2)
    ) if np.sum((y - y.mean()) ** 2) > 0 else np.nan

    return {
        "alpha": alpha,
        "beta": beta,
        "sigma": sigma,
        "n_est": len(y),
        "r2": r2,
        "market_mean": x.mean(),
        "sxx": sxx,
    }


def calculate_car(window, model):
    """Calcule le CAR et son test statistique."""
    if window.empty or window.isna().any().any():
        return None

    market = window["market_return"].to_numpy(dtype=float)
    abnormal = window["abnormal_return"].to_numpy(dtype=float)

    car = abnormal.sum()
    length = len(abnormal)

    variance = model["sigma"] ** 2 * (
        length
        + length / model["n_est"]
        + np.sum((market - model["market_mean"]) ** 2)
        / model["sxx"]
    )

    if variance <= 0:
        return None

    standard_error = np.sqrt(variance)
    t_stat = car / standard_error

    p_value = 2 * student_t.sf(
        abs(t_stat),
        df=model["n_est"] - 2,
    )

    return {
        "car": car,
        "t_stat": t_stat,
        "p_value": p_value,
        "n_days": length,
    }


def main():
    print("=" * 70)
    print("ÉTUDE D'ÉVÉNEMENTS QUOTIDIENNE - MODÈLE DE MARCHÉ")
    print("=" * 70)

    prices = load_prices()
    events = load_events()

    trading_dates = prices.index.to_numpy(dtype="datetime64[ns]")

    daily_results = []
    event_results = []

    skipped = 0

    for event in events.itertuples(index=False):
        ticker = event.ticker

        if ticker not in prices.columns:
            skipped += 1
            continue

        event_date = map_event_date(
            event.published_utc,
            trading_dates,
        )

        if event_date is None or event_date not in prices.index:
            skipped += 1
            continue

        event_idx = prices.index.get_loc(event_date)

        # Fenêtre d'estimation : J-120 à J-21.
        est_start = event_idx + ESTIMATION_START
        est_end = event_idx + ESTIMATION_END + 1

        if est_start < 0:
            skipped += 1
            continue

        estimation = prices.iloc[est_start:est_end]

        model = fit_market_model(
            estimation[ticker],
            estimation[BENCHMARK],
        )

        if model is None:
            skipped += 1
            continue

        # Fenêtre d'événement : J-5 à J+5.
        window_start = event_idx + EVENT_START
        window_end = event_idx + EVENT_END + 1

        if window_start < 0 or window_end > len(prices):
            skipped += 1
            continue

        window = prices.iloc[window_start:window_end][
            [ticker, BENCHMARK]
        ].copy()

        if window.isna().any().any():
            skipped += 1
            continue

        window = window.rename(columns={
            ticker: "stock_return",
            BENCHMARK: "market_return",
        })

        window["abnormal_return"] = (
            window["stock_return"]
            - model["alpha"]
            - model["beta"] * window["market_return"]
        )

        window["event_day"] = range(EVENT_START, EVENT_END + 1)

        metadata = {
            "article_id": event.article_id,
            "ticker": ticker,
            "published_utc": event.published_utc.isoformat(),
            "event_date": event_date.date().isoformat(),
        }

        for column in [
            "source", "publisher", "headline", "categorie",
            "sentiment_score", "sentiment_label",
        ]:
            if hasattr(event, column):
                metadata[column] = getattr(event, column)

        for key in ["alpha", "beta", "sigma", "n_est", "r2"]:
            metadata[key] = model[key]

        for date, row in window.iterrows():
            daily_results.append({
                **metadata,
                "date": date.date().isoformat(),
                "event_day": int(row["event_day"]),
                "stock_return": row["stock_return"],
                "market_return": row["market_return"],
                "abnormal_return": row["abnormal_return"],
            })

        # CAR sur plusieurs fenêtres.
        for label, start, end in [
            ("m1_p1", -1, 1),
            ("0_p1", 0, 1),
            ("m1_p5", -1, 5),
            ("m5_p5", -5, 5),
        ]:
            selected = window[
                window["event_day"].between(start, end)
            ]

            result = calculate_car(selected, model)

            if result is not None:
                event_results.append({
                    **metadata,
                    "window": label,
                    **result,
                })

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    daily_df = pd.DataFrame(daily_results)
    event_df = pd.DataFrame(event_results)

    daily_path = OUTPUT_DIR / "daily_abnormal_returns.csv"
    event_path = OUTPUT_DIR / "daily_car_results.csv"

    daily_df.to_csv(daily_path, index=False)
    event_df.to_csv(event_path, index=False)

    print("\nRÉSULTATS")
    print("-" * 70)
    print(f"Événements reçus       : {len(events):,}")
    print(f"Événements ignorés     : {skipped:,}")
    print(f"Événements traités     : {daily_df['article_id'].nunique() if not daily_df.empty else 0:,}")
    print(f"Observations AR        : {len(daily_df):,}")
    print(f"Résultats CAR          : {len(event_df):,}")

    if not event_df.empty:
        print("\nCAR PAR FENÊTRE")
        print(
            event_df.groupby("window")["car"]
            .agg(["count", "mean", "median", "std"])
            .to_string()
        )

        print("\nPROPORTION DE CAR SIGNIFICATIFS À 5 %")
        print(
            event_df.assign(
                significant=event_df["p_value"] < 0.05
            )
            .groupby("window")["significant"]
            .mean()
            .to_string()
        )

    print("\nFICHIERS CRÉÉS")
    print(daily_path)
    print(event_path)


if __name__ == "__main__":
    main()
