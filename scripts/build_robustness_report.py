from pathlib import Path
import pandas as pd

from nmp.local.events_batch import (
    effect,
    summarize,
)
from nmp.local.robustness import compare_to_placebo

BASE = Path("data/exports")

real = pd.read_csv(BASE / "event_results_batch.csv")
placebo = pd.read_csv(BASE / "event_placebo.csv")
categories = pd.read_csv(BASE / "event_by_category.csv")

rows = []

# 1. Résultats principaux
main = summarize(real)

for col in ("car_0_15", "car_0_60"):
    result = main.get(col, {})
    for metric, value in result.items():
        rows.append({
            "analyse": "principale",
            "fenetre": col,
            "regroupement": "evenement",
            "indicateur": metric,
            "valeur": value,
        })

# 2. Comparaison réel / placebo : regroupement titre-jour
for col in ("car_0_15", "car_0_60"):
    result = compare_to_placebo(real, placebo, col=col)
    for metric, value in result.items():
        rows.append({
            "analyse": "placebo",
            "fenetre": col,
            "regroupement": "titre-jour",
            "indicateur": metric,
            "valeur": value,
        })

# 3. Comparaison réel / placebo : regroupement par date
clean_real = real[~real["leak_flag"].astype(bool)].copy()
clean_placebo = placebo[~placebo["leak_flag"].astype(bool)].copy()

for col in ("car_0_15", "car_0_60"):
    p = clean_placebo.assign(amp=clean_placebo[col].abs())
    m = p.groupby("event_idx").agg(
        amp_placebo=("amp", "mean"),
        n_placebo=("amp", "size"),
    )

    j = clean_real.assign(amp_reel=clean_real[col].abs()).join(m, how="inner")
    j = j[j["n_placebo"] >= 3].copy()
    j["ecart"] = j["amp_reel"] - j["amp_placebo"]
    j["jour"] = pd.to_datetime(j["published_utc"], utc=True).dt.date

    result = effect(j.groupby("jour")["ecart"].mean())

    for metric, value in result.items():
        rows.append({
            "analyse": "placebo",
            "fenetre": col,
            "regroupement": "date",
            "indicateur": metric,
            "valeur": value,
        })

# 4. Résultats par catégorie déjà calculés
for _, r in categories.iterrows():
    for metric in ("evenements", "titres_jours", "mean",
                   "ci_low", "ci_high", "p_ttest", "p_holm"):
        if metric in categories.columns:
            rows.append({
                "analyse": f"categorie:{r['categorie']}",
                "fenetre": "car_0_60",
                "regroupement": "titre-jour",
                "indicateur": metric,
                "valeur": r[metric],
            })

summary = pd.DataFrame(rows)
csv_path = BASE / "robustness_summary.csv"
md_path = BASE / "robustness_report.md"

summary.to_csv(csv_path, index=False)

# Rapport lisible
lines = [
    "# Bilan de robustesse — Projet NMP",
    "",
    "## Données utilisées",
    f"- Événements calculés : {len(real)}",
    f"- Événements sans leak_flag : {(~real['leak_flag'].astype(bool)).sum()}",
    f"- Placebos disponibles : {len(placebo)}",
    f"- Placebos sans leak_flag : {(~placebo['leak_flag'].astype(bool)).sum()}",
    "",
    "## Résultats principaux",
    "```",
    str(pd.DataFrame([
        {"fenetre": col, **main.get(col, {})}
        for col in ("car_0_15", "car_0_60")
    ])),
    "```",
    "",
    "## Comparaison réel / placebo",
]

for col in ("car_0_15", "car_0_60"):
    lines.extend([f"### {col}", ""])
    for grouping in ("titre-jour", "date"):
        sub = summary[
            (summary["analyse"] == "placebo")
            & (summary["fenetre"] == col)
            & (summary["regroupement"] == grouping)
        ]
        values = dict(zip(sub["indicateur"], sub["valeur"]))
        lines.append(f"**Regroupement : {grouping}**")
        if grouping == "titre-jour":
            metrics = [
                ("n_evenements", "n"),
                ("ecart_mean", "mean"),
                ("ecart_ci_low", "ci_low"),
                ("ecart_ci_high", "ci_high"),
                ("ecart_p_ttest", "p_ttest"),
                ("ecart_p_wilcoxon", "p_wilcoxon"),
                ("ecart_mde", "mde"),
            ]
        else:
            metrics = [(m, m) for m in (
                "n", "mean", "ci_low", "ci_high",
                "p_ttest", "p_wilcoxon", "mde"
            )]

        for key, label in metrics:
            if key in values:
                lines.append(f"- {label} : {values[key]}")
        lines.append("")

lines.extend([
    "## Résultats par catégorie",
    "",
    categories.to_string(index=False),
    "",
    "## Réserves méthodologiques",
    "- Les tests t et Wilcoxon peuvent conduire à des conclusions différentes.",
    "- Le regroupement par date ne traite pas toute la dépendance entre titres.",
    "- Les résultats doivent être interprétés comme exploratoires, et non causaux.",
    "- Les tests multiples doivent être pris en compte dans l'interprétation.",
    "",
    "## Traçabilité",
    "- Sources : event_results_batch.csv, event_placebo.csv, event_by_category.csv.",
    "- Aucun CAR n'a été recalculé.",
])

md_path.write_text("\n".join(lines), encoding="utf-8")

print(f"CSV créé : {csv_path}")
print(f"Rapport créé : {md_path}")
print(f"Nombre de lignes consolidées : {len(summary)}")
