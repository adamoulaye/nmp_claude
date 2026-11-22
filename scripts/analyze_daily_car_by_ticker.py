"""Analyse des CAR par titre boursier et fenêtre d'événement."""

from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import ttest_1samp


INPUT_FILE = Path("data/exports/daily_event_study/daily_car_results.csv")
OUTPUT_DIR = Path("data/exports/daily_event_study")


def benjamini_hochberg(pvalues):
    """Correction BH en conservant les valeurs manquantes."""
    p = np.asarray(pvalues, dtype=float)
    result = np.full(len(p), np.nan)

    valid = np.isfinite(p)
    pv = p[valid]
    n = len(pv)

    if n == 0:
        return result

    order = np.argsort(pv)
    ranked = pv[order]

    adjusted = ranked * n / np.arange(1, n + 1)
    adjusted = np.minimum.accumulate(adjusted[::-1])[::-1]
    adjusted = np.clip(adjusted, 0, 1)

    restored = np.empty(n)
    restored[order] = adjusted
    result[valid] = restored

    return result


def main():
    print("=" * 75)
    print("ANALYSE DES CAR PAR TITRE BOURSIER")
    print("=" * 75)

    df = pd.read_csv(INPUT_FILE)

    required = {"ticker", "window", "car"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Colonnes manquantes : {missing}")

    df["car"] = pd.to_numeric(df["car"], errors="coerce")
    df = df.dropna(subset=["ticker", "window", "car"]).copy()

    results = []

    for (ticker, window), group in df.groupby(["ticker", "window"]):
        values = group["car"].to_numpy(dtype=float)
        n = len(values)

        if n >= 2:
            test = ttest_1samp(values, popmean=0)
            p_value = float(test.pvalue)
        else:
            p_value = np.nan

        results.append({
            "ticker": ticker,
            "window": window,
            "n": n,
            "car_mean": values.mean(),
            "car_median": np.median(values),
            "car_std": values.std(ddof=1) if n >= 2 else np.nan,
            "pct_negative": (values < 0).mean() * 100,
            "pct_positive": (values > 0).mean() * 100,
            "p_value": p_value,
        })

    summary = pd.DataFrame(results)

    # Correction BH séparément pour chaque fenêtre.
    summary["p_value_bh"] = np.nan

    for _window, indexes in summary.groupby("window").groups.items():
        summary.loc[indexes, "p_value_bh"] = benjamini_hochberg(
            summary.loc[indexes, "p_value"].to_numpy()
        )

    summary["significant_bh_5pct"] = summary["p_value_bh"] < 0.05

    summary = summary.sort_values(["window", "car_mean"])

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / "car_by_ticker.csv"
    summary.to_csv(output_file, index=False)

    print("\nRÉSULTATS PAR FENÊTRE")
    print("-" * 75)

    for window in ["0_p1", "m1_p1", "m1_p5", "m5_p5"]:
        part = summary[summary["window"] == window]

        if part.empty:
            continue

        print(f"\nFENÊTRE : {window}")
        print(
            part[
                [
                    "ticker", "n", "car_mean", "car_median",
                    "pct_negative", "p_value_bh",
                    "significant_bh_5pct",
                ]
            ].to_string(
                index=False,
                formatters={
                    "car_mean": "{:.2%}".format,
                    "car_median": "{:.2%}".format,
                    "pct_negative": "{:.1f}%".format,
                    "p_value_bh": "{:.4f}".format,
                },
            )
        )

    print("\nSYNTHÈSE")
    print("-" * 75)

    print(
        summary.groupby("window").agg(
            titres_analyses=("ticker", "nunique"),
            titres_significatifs=("significant_bh_5pct", "sum"),
            car_moyen=("car_mean", "mean"),
            part_titres_car_negatif=("car_mean", lambda x: (x < 0).mean() * 100),
        ).to_string(
            formatters={
                "car_moyen": "{:.2%}".format,
                "part_titres_car_negatif": "{:.1f}%".format,
            }
        )
    )

    print("\nFICHIER CRÉÉ")
    print(output_file)


if __name__ == "__main__":
    main()
