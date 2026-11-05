"""Modèle 2b : classification de l'ampleur (faible/moyenne/forte) — validation chronologique + baseline."""

from __future__ import annotations

import numpy as np
import pandas as pd

CAT_FEATURES = ["source", "sentiment_label", "category", "session_type", "hour_bucket"]
NUM_FEATURES = ["sentiment_score", "cluster_size", "n_tickers", "pre_vol_15m", "is_first_i"]


def make_target(abs_car: pd.Series, train_mask: pd.Series) -> pd.Series:
    """Terciles calculés sur le TRAIN seulement (pas de fuite de distribution)."""
    q1, q2 = abs_car[train_mask].quantile([1 / 3, 2 / 3])
    return pd.Series(np.select([abs_car <= q1, abs_car <= q2], [0, 1], 2), index=abs_car.index, name="magnitude")


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["hour_bucket"] = pd.to_datetime(d["published_utc"], utc=True).dt.tz_convert("America/New_York").dt.hour.astype(str)
    d["is_first_i"] = d["is_first"].astype(int)
    return d


def build_models(seed: int = 42):
    from sklearn.compose import ColumnTransformer
    from sklearn.dummy import DummyClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from xgboost import XGBClassifier

    prep = ColumnTransformer(
        [
            ("c", OneHotEncoder(handle_unknown="ignore", min_frequency=10), CAT_FEATURES),
            ("n", StandardScaler(), NUM_FEATURES),
        ]
    )
    return {
        "baseline_prior": DummyClassifier(strategy="prior"),
        "baseline_logreg": Pipeline([("p", prep), ("m", LogisticRegression(max_iter=1000, class_weight="balanced"))]),
        "xgboost": Pipeline(
            [
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
                        eval_metric="mlogloss",
                        tree_method="hist",
                        random_state=seed,
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
        "random_forest": Pipeline(
            [
                ("p", prep),
                (
                    "m",
                    __import__("sklearn.ensemble", fromlist=["x"]).RandomForestClassifier(
                        n_estimators=400,
                        min_samples_leaf=5,
                        class_weight="balanced_subsample",
                        n_jobs=-1,
                        random_state=seed,
                    ),
                ),
            ]
        ),
    }


def train_and_log(df: pd.DataFrame, experiment: str, registered_name: str, n_splits: int = 5, holdout: float = 0.2) -> dict:
    """df : une ligne par (article first, ticker) avec abs_car, features, published_utc.

    Validation : TimeSeriesSplit avec `gap` (≥ fenêtre de l'événement) + hold-out final chronologique.
    """
    import mlflow
    from sklearn.metrics import balanced_accuracy_score, f1_score, log_loss
    from sklearn.model_selection import TimeSeriesSplit

    d = prepare(df.sort_values("published_utc").reset_index(drop=True))
    cut = int(len(d) * (1 - holdout))
    tr_mask = pd.Series(np.arange(len(d)) < cut, index=d.index)
    d["magnitude"] = make_target(d["abs_car"], tr_mask)
    tr, te = d[tr_mask], d[~tr_mask]
    X_cols = CAT_FEATURES + NUM_FEATURES
    mlflow.set_experiment(experiment)
    results = {}
    for name, model in build_models().items():
        with mlflow.start_run(run_name=name) as run:
            cv = []
            for a, b in TimeSeriesSplit(n_splits=n_splits, gap=50).split(tr):
                model.fit(tr.iloc[a][X_cols], tr.iloc[a]["magnitude"])
                cv.append(f1_score(tr.iloc[b]["magnitude"], model.predict(tr.iloc[b][X_cols]), average="macro"))
            model.fit(tr[X_cols], tr["magnitude"])
            proba, pred = model.predict_proba(te[X_cols]), model.predict(te[X_cols])
            m = {
                "cv_macro_f1_mean": float(np.mean(cv)),
                "cv_macro_f1_std": float(np.std(cv)),
                "holdout_macro_f1": float(f1_score(te["magnitude"], pred, average="macro")),
                "holdout_bal_acc": float(balanced_accuracy_score(te["magnitude"], pred)),
                "holdout_logloss": float(log_loss(te["magnitude"], proba, labels=[0, 1, 2])),
            }
            mlflow.log_params({"model": name, "n_train": len(tr), "n_test": len(te)})
            mlflow.log_metrics(m)
            mlflow.sklearn.log_model(
                model,
                "model",
                input_example=te[X_cols].head(3),
                registered_model_name=f"{registered_name}_{name}",
            )
            results[name] = {"run_id": run.info.run_id, **m}
    return results
