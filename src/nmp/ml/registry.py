"""Cycle de vie des modèles : challenger -> champion (alias MLflow) avec marge minimale."""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def decide_promotion(new: float, old: float | None, margin: float = 0.01) -> tuple[bool, str]:
    """Règle de promotion : le challenger doit battre le champion d'au moins `margin`.

    `old` vaut None quand il n'y a pas encore de champion : le challenger est alors promu.
    """
    if old is None:
        return True, f"aucun champion en place : promu (metrique {new:.4f})"
    if new >= old + margin:
        return True, f"promu : {new:.4f} >= {old:.4f} + marge {margin:.4f}"
    return False, f"refuse : {new:.4f} < {old:.4f} + marge {margin:.4f} (ecart {new - old:+.4f})"


def _metric(client, name: str, version: str, metric: str) -> float:
    run_id = client.get_model_version(name, version).run_id
    metrics = client.get_run(run_id).data.metrics
    if metric not in metrics:
        raise ValueError(f"Metrique {metric!r} absente du run {run_id} ({name} v{version})")
    return float(metrics[metric])


def promote_if_better(
    name: str,
    challenger_version: str,
    metric: str = "holdout_macro_f1",
    margin: float = 0.01,
    client=None,
    dry_run: bool = False,
) -> bool:
    """Promeut `challenger_version` au rang de champion si la règle le permet.

    Avec dry_run=True, la décision est calculée et journalisée sans modifier aucun alias.
    """
    if client is None:
        from mlflow.tracking import MlflowClient

        client = MlflowClient()
    new = _metric(client, name, challenger_version, metric)
    try:
        champ = client.get_model_version_by_alias(name, "champion")
        old = _metric(client, name, champ.version, metric)
    except Exception:
        old = None
    ok, why = decide_promotion(new, old, margin)
    log.info("Promotion %s v%s (%s) : %s", name, challenger_version, metric, why)
    if not dry_run:
        client.set_registered_model_alias(name, "challenger", challenger_version)
        if ok:
            client.set_registered_model_alias(name, "champion", challenger_version)
    return ok
