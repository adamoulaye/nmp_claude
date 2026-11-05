"""Cycle de vie des modèles : challenger -> champion (alias MLflow) avec marge minimale."""

from __future__ import annotations


def promote_if_better(name: str, challenger_version: str, metric: str = "holdout_macro_f1", margin: float = 0.01) -> bool:
    from mlflow.tracking import MlflowClient

    c = MlflowClient()
    c.set_registered_model_alias(name, "challenger", challenger_version)
    new = c.get_run(c.get_model_version(name, challenger_version).run_id).data.metrics[metric]
    try:
        champ = c.get_model_version_by_alias(name, "champion")
        old = c.get_run(champ.run_id).data.metrics[metric]
    except Exception:
        old = float("-inf")
    if new >= old + margin:
        c.set_registered_model_alias(name, "champion", challenger_version)
        return True
    return False
