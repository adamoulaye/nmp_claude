
"""Calcul batch des CAR par article, titre et reference temporelle."""

from __future__ import annotations

import pandas as pd

from nmp import __version__ as CODE_VERSION
from nmp.transform.event_study import (
    IncompleteEventError,
    compute_event,
)

BENCHMARK = "SPY"

WINDOWS = (
    (15, "car_0_15", "t_0_15", "p_0_15"),
    (60, "car_0_60", "t_0_60", "p_0_60"),
)


def run_batch(
    news: pd.DataFrame,
    bars: pd.DataFrame,
    benchmark: str = BENCHMARK,
    *,
    id_col: str = "article_id",
    ticker_col: str = "ticker",
    ts_col: str = "published_utc",
    keep: tuple[str, ...] = (),
):
    """Calcule les CAR et distingue les evenements incomplets."""

    required_news = {id_col, ticker_col, ts_col}
    missing_news = required_news.difference(news.columns)

    if missing_news:
        raise ValueError(
            f"Colonnes manquantes dans news : {sorted(missing_news)}"
        )

    required_bars = {"ticker", "ts_utc", "close", "volume"}
    missing_bars = required_bars.difference(bars.columns)

    if missing_bars:
        raise ValueError(
            f"Colonnes manquantes dans bars : {sorted(missing_bars)}"
        )

    missing_keep = set(keep).difference(news.columns)

    if missing_keep:
        raise ValueError(
            f"Colonnes keep absentes de news : {sorted(missing_keep)}"
        )

    by = {
        ticker: group.sort_values("ts_utc")
        for ticker, group in bars.groupby("ticker")
    }

    results = []
    windows = []
    errors = []

    for row in news.to_dict("records"):
        key = row.get(id_col)
        ticker = row.get(ticker_col)

        try:
            if pd.isna(key):
                raise ValueError("Identifiant article absent.")

            if pd.isna(ticker):
                raise ValueError("Ticker absent.")

            if pd.isna(row.get(ts_col)):
                raise ValueError("Horodatage de l'evenement absent.")

            if ticker not in by:
                raise ValueError(
                    f"Aucune barre disponible pour {ticker}"
                )

            if benchmark not in by:
                raise ValueError(
                    f"Benchmark absent : {benchmark}"
                )

            summary, window = compute_event(
                by[ticker],
                by[benchmark],
                row[ts_col],
            )

        except IncompleteEventError as exc:
            errors.append({
                id_col: key,
                "error": str(exc)[:200],
                "status": "pending",
            })
            continue

        except (KeyError, ValueError, TypeError) as exc:
            errors.append({
                id_col: key,
                "error": f"{type(exc).__name__}: {exc}"[:200],
                "status": "error",
            })
            continue

        result = {
            id_col: key,
            ticker_col: ticker,
            ts_col: row[ts_col],
            **{
                column: row[column]
                for column in keep
            },
            **summary,
            "code_version": CODE_VERSION,
            "computed_at": pd.Timestamp.now(tz="UTC"),
        }

        results.append(result)

        windows.append(
            window.assign(**{
                id_col: key,
                ticker_col: ticker,
            })
        )

    all_windows = (
        pd.concat(windows, ignore_index=True)
        if windows
        else pd.DataFrame()
    )

    return (
        pd.DataFrame(results),
        all_windows,
        pd.DataFrame(
            errors,
            columns=[id_col, "error", "status"],
        ),
    )


def to_bv_event_car(res: pd.DataFrame) -> pd.DataFrame:
    """Transforme les resultats CAR en deux lignes par evenement."""

    if res.empty:
        return pd.DataFrame()

    res = res.copy()

    if "event_time_basis" not in res.columns:
        res["event_time_basis"] = "published"

    required = [
        "hk_article",
        "hk_security",
        "event_time_basis",
        "car_0_15",
        "t_0_15",
        "p_0_15",
        "car_0_60",
        "t_0_60",
        "p_0_60",
        "alpha",
        "beta",
        "n_est",
        "car_pre",
        "leak_flag",
        "computed_at",
    ]

    missing = [
        column
        for column in required
        if column not in res.columns
    ]

    if missing:
        raise ValueError(
            f"Colonnes manquantes pour bv_event_car : {missing}"
        )

    records = []

    for row in res.to_dict("records"):
        for window_end, car_col, t_col, p_col in WINDOWS:
            records.append({
                "hk_article": row["hk_article"],
                "hk_security": row["hk_security"],
                "event_time_basis": row["event_time_basis"],
                "window_start_min": 1,
                "window_end_min": window_end,
                "car": row[car_col],
                "t_stat": row[t_col],
                "p_value": row[p_col],
                "alpha": row["alpha"],
                "beta": row["beta"],
                "n_est": row["n_est"],
                "car_pre": row["car_pre"],
                "leak_flag": bool(row["leak_flag"]),
                "model_version": f"market_model_{CODE_VERSION}",
                "load_ts": row["computed_at"],
            })

    return pd.DataFrame(records)
