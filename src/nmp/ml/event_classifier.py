
"""Modèle 2b : classification de l'ampleur des réactions boursières."""

from __future__ import annotations

import numpy as np
import pandas as pd

CAT_FEATURES = [
    "source",
    "sentiment_label",
    "category",
    "session_type",
    "hour_bucket",
]

NUM_FEATURES = [
    "sentiment_score",
    "n_tickers",
    "pre_vol_15m",
    "is_first_i",
]


def make_target(
    abs_car: pd.Series,
    train_mask: pd.Series,
) -> pd.Series:
    """Construit les classes faible, moyen et élevé à partir des terciles du train."""
    q1, q2 = abs_car[train_mask].quantile([1 / 3, 2 / 3])

    return pd.Series(
        np.select(
            [abs_car <= q1, abs_car <= q2],
            [0, 1],
            default=2,
        ),
        index=abs_car.index,
        name="magnitude",
    )


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Crée les variables dérivées à partir des colonnes sources."""
    d = df.copy()

    d["hour_bucket"] = (
        pd.to_datetime(d["published_utc"], utc=True)
        .dt.tz_convert("America/New_York")
        .dt.hour.astype(str)
    )

    d["is_first_i"] = d["is_first"].astype(int)

    return d


def build_models(seed: int = 42):
    from sklearn.compose import ColumnTransformer
    from sklearn.dummy import DummyClassifier
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from xgboost import XGBClassifier

    prep = ColumnTransformer(
        [
            (
                "c",
                OneHotEncoder(
                    handle_unknown="ignore",
                    min_frequency=10,
                ),
                CAT_FEATURES,
            ),
            ("n", StandardScaler(), NUM_FEATURES),
        ]
    )

    return {
        "baseline_prior": DummyClassifier(strategy="prior"),

        "baseline_logreg": Pipeline([
            ("p", prep),
            (
                "m",
                LogisticRegression(
                    max_iter=1000,
                    class_weight="balanced",
                ),
            ),
        ]),

        "xgboost": Pipeline([
            ("p", prep),
            (
                "m",
                XGBClassifier(
                    n_estimators=400,
                    max_depth=5,
                    learning_rate=0.05,
                    subsample=0.8,
                    colsample_bytree=0.8,
                    objective="multi:softprob",
                    num_class=3,
                    eval_metric="mlogloss",
                    tree_method="hist",
                    random_state=seed,
                    n_jobs=-1,
                ),
            ),
        ]),

        "random_forest": Pipeline([
            ("p", prep),
            (
                "m",
                RandomForestClassifier(
                    n_estimators=400,
                    min_samples_leaf=5,
                    class_weight="balanced_subsample",
                    n_jobs=-1,
                    random_state=seed,
                ),
            ),
        ]),
    }


def train_and_log(
    df: pd.DataFrame,
    experiment: str,
    registered_name: str,
    n_splits: int = 5,
    holdout: float = 0.2,
) -> dict:
    """Classification chronologique avec hold-out et CV conditionnelle."""
    import mlflow
    from sklearn.metrics import (
        balanced_accuracy_score,
        f1_score,
        log_loss,
    )
    from sklearn.model_selection import TimeSeriesSplit

    if df.empty:
        raise ValueError("Le jeu de données d'entraînement est vide.")

    # Colonnes sources réellement attendues dans gold.v_training_set.
    # hour_bucket et is_first_i sont créées par prepare().
    required = [
        "published_utc",
        "abs_car",
        "source",
        "sentiment_label",
        "category",
        "session_type",
        "sentiment_score",
        "n_tickers",
        "pre_vol_15m",
        "is_first",
    ]

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(
            f"Colonnes manquantes dans gold.v_training_set : {missing}"
        )

    if not 0 < holdout < 1:
        raise ValueError("holdout doit être compris entre 0 et 1.")

    d = prepare(
        df.sort_values("published_utc").reset_index(drop=True)
    )

    d = d.replace([np.inf, -np.inf], np.nan)

    d = d.dropna(
        subset=["published_utc", "abs_car"]
        + CAT_FEATURES
        + NUM_FEATURES
    )

    if len(d) < 15:
        raise ValueError(
            f"Échantillon insuffisant : {len(d)} observations. "
            "Il faut au moins 15 observations."
        )

    cut = int(len(d) * (1 - holdout))

    if cut <= 0 or cut >= len(d):
        raise ValueError("Découpage chronologique invalide.")

    train_mask = pd.Series(
        np.arange(len(d)) < cut,
        index=d.index,
    )

    d["magnitude"] = make_target(d["abs_car"], train_mask)

    tr = d.iloc[:cut].copy()
    te = d.iloc[cut:].copy()

    # Les trois classes doivent être présentes dans le train.
    train_classes = sorted(tr["magnitude"].unique().tolist())

    if train_classes != [0, 1, 2]:
        distribution = (
            tr["magnitude"]
            .value_counts()
            .sort_index()
            .to_dict()
        )
        raise ValueError(
            "Les trois classes (0, 1, 2) ne sont pas présentes "
            f"dans le jeu d'entraînement. Distribution : {distribution}. "
            "Il faut davantage de données ou revoir les seuils des terciles."
        )

    X_cols = CAT_FEATURES + NUM_FEATURES

    mlflow.set_experiment(experiment)
    results = {}

    for name, model in build_models().items():
        cv = []

        # La validation croisée est activée seulement avec un échantillon
        # suffisamment grand pour absorber les 50 observations de séparation.
        if len(tr) >= 400:
            splitter = TimeSeriesSplit(
                n_splits=n_splits,
                gap=50,
            )

            for a, b in splitter.split(tr):
                fold_train = tr.iloc[a]
                fold_valid = tr.iloc[b]

                if sorted(fold_train["magnitude"].unique()) != [0, 1, 2]:
                    continue

                model.fit(
                    fold_train[X_cols],
                    fold_train["magnitude"],
                )

                pred_cv = model.predict(fold_valid[X_cols])

                cv.append(
                    f1_score(
                        fold_valid["magnitude"],
                        pred_cv,
                        average="macro",
                        zero_division=0,
                    )
                )

        with mlflow.start_run(run_name=name) as run:
            model.fit(tr[X_cols], tr["magnitude"])

            pred = model.predict(te[X_cols])
            proba = model.predict_proba(te[X_cols])

            metrics = {
                "holdout_macro_f1": float(
                    f1_score(
                        te["magnitude"],
                        pred,
                        average="macro",
                        zero_division=0,
                    )
                ),
                "holdout_bal_acc": float(
                    balanced_accuracy_score(
                        te["magnitude"],
                        pred,
                    )
                ),
                "holdout_logloss": float(
                    log_loss(
                        te["magnitude"],
                        proba,
                        labels=[0, 1, 2],
                    )
                ),
            }

            if cv:
                metrics["cv_macro_f1_mean"] = float(np.mean(cv))
                metrics["cv_macro_f1_std"] = float(np.std(cv))

            mlflow.log_params({
                "model": name,
                "n_train": len(tr),
                "n_test": len(te),
                "n_total": len(d),
                "n_splits": n_splits if cv else 0,
                "gap": 50 if cv else 0,
                "validation": "chronological_holdout",
            })

            mlflow.log_metrics(metrics)

            mlflow.sklearn.log_model(
                model,
                "model",
                input_example=te[X_cols].head(3),
                registered_model_name=f"{registered_name}_{name}",
            )

            results[name] = {
                "run_id": run.info.run_id,
                **metrics,
            }

    return results
