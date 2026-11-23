"""Journal d'exécution : volumes reçus / retenus / écartés, durée, résultat."""

from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from nmp.common.retry import end_quota_observation, start_quota_observation

log = logging.getLogger("nmp.runlog")


@dataclass
class RunStats:
    job: str = ""
    source: str = ""
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_s: float = 0.0
    received: int = 0
    kept: int = 0
    rejected: int = 0
    status: str = "RUNNING"  # SUCCESS | FAILED | QUOTA | SCHEMA_DRIFT
    message: str = ""
    quota_remaining: int = -1


@contextmanager
def run_log(spark, catalog: str, job: str, source: str = ""):
    from nmp.common.retry import QuotaExceeded, SchemaDrift

    stats = RunStats(job=job, source=source)
    token = start_quota_observation()
    t0 = time.monotonic()
    try:
        yield stats
        if stats.status == "RUNNING":
            stats.status = "SUCCESS"
    except Exception as exc:
        stats.status = "QUOTA" if isinstance(exc, QuotaExceeded) else "SCHEMA_DRIFT" if isinstance(exc, SchemaDrift) else "FAILED"
        stats.message = f"{type(exc).__name__}: {exc}"[:2000]
        raise
    finally:
        obs = end_quota_observation(token)
        if "remaining" in obs:
            stats.quota_remaining = obs["remaining"]
        stats.duration_s = round(time.monotonic() - t0, 2)
        log.info(json.dumps(asdict(stats), default=str))
        if spark is not None:
            spark.createDataFrame([asdict(stats)]).write.mode("append").saveAsTable(f"{catalog}.ops.run_log")
            if "limit" in obs:
                row = [(datetime.now(timezone.utc), stats.source, obs["limit"] - obs["remaining"], obs["limit"])]
                schema = "checked_utc timestamp, source string, used bigint, quota bigint"
                spark.createDataFrame(row, schema).write.mode("append").saveAsTable(f"{catalog}.ops.api_quota")
