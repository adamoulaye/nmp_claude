"""Couche Bronze: reponses brutes en AJOUT SEULEMENT + journal d'execution."""

import json
from datetime import datetime, timezone

from .config import DATA_DIR


def save_raw(source: str, key: str, payload, params: dict | None = None):
    """Ecrit la reponse brute telle quelle. Ne modifie ni ne supprime jamais un fichier existant."""
    now = datetime.now(timezone.utc)
    batch_id = now.strftime("%Y%m%dT%H%M%S%fZ")
    folder = DATA_DIR / "bronze" / source / key / now.strftime("%Y-%m-%d")
    folder.mkdir(parents=True, exist_ok=True)
    record = {
        "source": source,
        "key": key,
        "params": params or {},
        "ingested_at": now.isoformat(),
        "batch_id": batch_id,
        "payload": payload,
    }
    path = folder / f"{batch_id}.json"
    path.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
    return path


def log_run(source, key, n_received, started, status="ok", error=None):
    """Une ligne JSON par execution: volumes recus, duree, resultat."""
    path = DATA_DIR / "logs" / "runs.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    rec = {
        "source": source,
        "key": key,
        "n_received": n_received,
        "status": status,
        "error": error,
        "started_at": started.isoformat(),
        "duration_s": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
