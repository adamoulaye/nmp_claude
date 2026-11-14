"""Analyse des CAR quotidiens par catégorie et sentiment."""

from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import ttest_1samp


INPUT_FILE = Path("data/exports/daily_event_study/daily_car_results.csv")
OUTPUT_DIR = Path("data/exports/daily_event_study")


def benjamini_hochberg(p_values):
    """Correction des p-values selon Benjamini-Hochberg."""
    p = np.asarray(p_values, dtype=float)
    adjusted = np.full(len(p), np.nan)

    valid = np.isfinite(p)
    values = p[valid]

    if len(values) == 0:
        return adjusted

    order = np.argsort(values)
    ranked = values[order]
    m = len(ranked)

    corrected = ranked * m / np.arange(1, m + 1)
    corrected = np.minimum.accumulate(corrected[::-1])[::-1]
    corrected = np.clip(corrected, 0, 1)

    restored = np.empty(m)
    restored[order] = corrected
    adjusted[valid] = restored

    return adjusted


def analyze_dimension(df, dimension):
    """Calcule les statistiques CAR pour une dimension."""
    results = []

    data = df.copy()
    data[dimension] = (
        data[dimension]
        .fillna("Non renseigné")
        .astype(str)
        .str.strip()
        .replace("", "Non renseigné")
    )

    for (group, window), subset in data.groupby(
        [dimension, "window"], observed=True
    ):
        values = pd.to_numeric(subset["car"], errors="coerce").dropna()

        if len(values) < 2:
            p_value = np.nan
        else:
            p_value = float(ttest_1samp(values, popmean=0).pvalue)

        results.append({
            dimension: group,
            "window": window,
            "n": len(values),
            "car_mean": values.mean(),
            "car_median": values.median(),
            "car_std": values.std(),
            "car_min": values.min(),
            "car_max": values.max(),
            "proportion_positive": (values > 0).mean(),
            "proportion_negative": (values < 0).mean(),
            "proportion_significant_individual": (
                (subset.loc[values.index, "p_value"] < 0.05).mean()
                if len(values) else np.nan
            ),
            "p_value_mean_car": p_value,
        })

    result = pd.DataFrame(results)

    # Correction BH séparément pour chaque fenêtre.
    result["p_value_bh"] = np.nan

    for window, indexes in result.groupby("window").groups.items():
        result.loc[indexes, "p_value_bh"] = benjamini_hochberg(
            result.loc[indexes, "p_value_mean_car"]
        )

    result["significant_bh_5pct"] = result["p_value_bh"] < 0.05

    return result.sort_values(
        ["window", "car_mean"],
        ascending=[True, False]
    )


def main():
    print("=" * 70)
    print("ANALYSE DES CAR PAR CATÉGORIE ET SENTIMENT")
    print("=" * 70)

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Fichier absent : {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    required = ["car", "window", "categorie", "sentiment_label", "p_value"]
    missing = set(required) - set(df.columns)

    if missing:
        raise ValueError(f"Colonnes absentes : {sorted(missing)}")

    df["car"] = pd.to_numeric(df["car"], errors="coerce")
    df["p_value"] = pd.to_numeric(df["p_value"], errors="coerce")
    df = df.dropna(subset=["car", "window"])

    category_results = analyze_dimension(df, "categorie")
    sentiment_results = analyze_dimension(df, "sentiment_label")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    category_path = OUTPUT_DIR / "car_by_category.csv"
    sentiment_path = OUTPUT_DIR / "car_by_sentiment.csv"

    category_results.to_csv(category_path, index=False)
    sentiment_results.to_csv(sentiment_path, index=False)

    for title, result, dimension in [
        ("CATÉGORIES", category_results, "categorie"),
        ("SENTIMENTS", sentiment_results, "sentiment_label"),
    ]:
        print(f"\n{title}")
        print("-" * 70)

        if result.empty:
            print("Aucun résultat.")
            continue

        display = result[
            [
                dimension, "window", "n", "car_mean",
                "car_median", "p_value_mean_car",
                "p_value_bh", "significant_bh_5pct"
            ]
        ].copy()

        for column in ["car_mean", "car_median"]:
            display[column] = display[column].map(
                lambda x: f"{x:.4%}" if pd.notna(x) else ""
            )

        for column in ["p_value_mean_car", "p_value_bh"]:
            display[column] = display[column].map(
                lambda x: f"{x:.4f}" if pd.notna(x) else ""
            )

        print(display.to_string(index=False))

    print("\nFICHIERS CRÉÉS")
    print(category_path)
    print(sentiment_path)


if __name__ == "__main__":
    main()
