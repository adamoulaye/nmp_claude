from pathlib import Path
import pandas as pd
from pprint import pprint

from nmp.local.robustness import compare_to_placebo

BASE = Path("data/exports")

real = pd.read_csv(BASE / "event_results_batch.csv")
placebo = pd.read_csv(BASE / "event_placebo.csv")

print("=" * 65)
print("COMPARAISON ENTRE ÉVÉNEMENTS RÉELS ET PLACEBO")
print("=" * 65)

print(f"\nÉvénements réels : {len(real)}")
print(f"Placebos disponibles : {len(placebo)}")
print(f"Événements réels sans leak : {(~real['leak_flag'].astype(bool)).sum()}")
print(f"Placebos sans leak : {(~placebo['leak_flag'].astype(bool)).sum()}")

for col in ["car_0_15", "car_0_60"]:
    print(f"\n{'-' * 65}")
    print(f"FENÊTRE : {col}")
    print("-" * 65)

    result = compare_to_placebo(
        real,
        placebo,
        col=col,
        min_placebos=3
    )

    pprint(result, sort_dicts=False)

    if result.get("n_evenements", 0) == 0:
        print("Attention : aucun événement admissible pour cette comparaison.")
    elif "ecart_mean" in result:
        print("Écart moyen :", result["ecart_mean"])
