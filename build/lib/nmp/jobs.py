"""Tâches Databricks (python_wheel_task) : ingestion batch, enrichissement, ML, santé."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from nmp.common.config import get_settings

log = logging.getLogger(__name__)


def _spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def ingest(source: str, lookback_days: int = 3) -> None:
    from nmp.ingestion.base import run_source
    from nmp.ingestion.sources import REGISTRY

    s = get_settings()
    start = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    run_source(_spark(), REGISTRY[source](s), default_start=start)


def enrich(lookback_hours: int = 72) -> None:
    """Silver -> Business Vault : dédoublonnage, séance, sentiment FinBERT, catégorie."""
    import pandas as pd

    from nmp.common.runlog import run_log
    from nmp.common.timeutil import attach_session, build_sessions
    from nmp.ml.sentiment import FinBertScorer
    from nmp.transform.dedup import cluster_articles

    s, spark = get_settings(), _spark()
    cat = s.catalog
    with run_log(spark, cat, "enrich") as st:
        since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        pdf = spark.sql(f"""
            SELECT hk_article, headline, summary, published_utc, updated_utc, first_seen_utc
            FROM {cat}.vault.v_article_latest WHERE published_utc >= TIMESTAMP'{since.isoformat()}'""").toPandas()
        st.received = len(pdf)
        if pdf.empty:
            return
        pdf["text"] = pdf["headline"].fillna("") + ". " + pdf["summary"].fillna("")
        d = cluster_articles(pdf)
        sess = build_sessions(
            (since - timedelta(days=10)).date().isoformat(),
            (datetime.now(timezone.utc) + timedelta(days=14)).date().isoformat(),
        )
        d = attach_session(d, sess)
        # disponibilité : jamais avant la 1re vue réelle (évite le biais d'anticipation en backfill/rejeu)
        d["available_utc"] = d[["published_utc", "first_seen_utc"]].max(axis=1)
        sc = FinBertScorer().score(d["text"].tolist())
        d = pd.concat([d.reset_index(drop=True), sc], axis=1)
        d["model_version"] = "finbert-prosus-v1"
        st.kept = len(d)
        spark.createDataFrame(d.drop(columns=["text"])).createOrReplaceTempView("_enriched")
        spark.sql(f"""MERGE INTO {cat}.business_vault.bv_article_enriched t USING _enriched s
                      ON t.hk_article = s.hk_article AND t.model_version = s.model_version
                      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def car(lookback_hours: int = 96) -> None:
    """Business Vault : CAR par (article, titre) — 1re publication EN SÉANCE, fenêtre [-15, +60] min.

    Uniquement les nouvelles publiées il y a plus de 65 min (fenêtre complète) et pas encore calculées -> idempotent.
    La logique pandas est testée (tests/test_car_batch.py) ; ce job-ci n'a PAS été exécuté sur Databricks.
    """
    import pandas as pd

    from nmp.common.runlog import run_log
    from nmp.transform.car_batch import run_batch, to_bv_event_car

    s, spark = get_settings(), _spark()
    cat = s.catalog
    with run_log(spark, cat, "car") as st:
        now = datetime.now(timezone.utc)
        since, until = now - timedelta(hours=lookback_hours), now - timedelta(minutes=65)
        events = spark.sql(f"""
            SELECT e.hk_article, l.hk_security, h.bk_symbol AS ticker, e.published_utc
            FROM {cat}.business_vault.bv_article_enriched e
            JOIN {cat}.vault.link_article_security l USING (hk_article)
            JOIN {cat}.vault.hub_security h USING (hk_security)
            WHERE e.is_first AND e.session_type = 'pendant'
              AND e.published_utc BETWEEN TIMESTAMP'{since.isoformat()}' AND TIMESTAMP'{until.isoformat()}'
              AND NOT EXISTS (SELECT 1 FROM {cat}.business_vault.bv_event_car c
                              WHERE c.hk_article = e.hk_article AND c.hk_security = l.hk_security AND c.window_end_min = 60)
        """).toPandas()
        st.received = len(events)
        if events.empty:
            return
        events["published_utc"] = pd.to_datetime(events["published_utc"], utc=True)
        lo, hi = (
            events["published_utc"].min() - timedelta(days=4),
            events["published_utc"].max() + timedelta(hours=2),
        )
        syms = ", ".join(f"'{t}'" for t in {*events["ticker"], s.benchmark})
        bars = spark.sql(f"""
            SELECT h.bk_symbol AS ticker, b.bar_ts AS ts_utc, b.close, b.volume
            FROM {cat}.gold.fact_bar_1m b JOIN {cat}.vault.hub_security h USING (hk_security)
            WHERE h.bk_symbol IN ({syms}) AND b.bar_ts BETWEEN TIMESTAMP'{lo.isoformat()}' AND TIMESTAMP'{hi.isoformat()}'
        """).toPandas()
        bars["ts_utc"] = pd.to_datetime(bars["ts_utc"], utc=True)
        res, _, err = run_batch(events, bars, s.benchmark, id_col="hk_article", keep=("hk_security",))
        st.kept, st.rejected = len(res), len(err)
        if err.shape[0]:
            log.warning("CAR non calculé pour %d événements (ex. %s)", len(err), err["error"].iloc[0])
        if res.empty:
            return
        out = to_bv_event_car(res)
        out["load_ts"] = out["load_ts"].dt.tz_convert("UTC").dt.tz_localize(None)
        spark.createDataFrame(out).createOrReplaceTempView("_car")
        spark.sql(f"""MERGE INTO {cat}.business_vault.bv_event_car t USING _car s
                      ON t.hk_article = s.hk_article AND t.hk_security = s.hk_security
                         AND t.window_end_min = s.window_end_min AND t.model_version = s.model_version
                      WHEN NOT MATCHED THEN INSERT *""")


def train(experiment: str = "/Shared/nmp/event_impact") -> None:
    from nmp.ml.event_classifier import train_and_log

    spark = _spark()
    df = spark.table(f"{get_settings().catalog}.gold.v_training_set").toPandas()
    res = train_and_log(df, experiment, f"{get_settings().catalog}.gold.event_magnitude")
    log.info(res)


def health() -> None:
    """Contrôles de fraîcheur -> table ops.freshness (alimente le rapport « État de la plateforme »)."""
    cat = get_settings().catalog
    _spark().sql(f"""
        INSERT INTO {cat}.ops.freshness
        SELECT current_timestamp(), source, max(published_utc), max(ingested_utc),
               (unix_timestamp(current_timestamp()) - unix_timestamp(max(ingested_utc)))/60.0
        FROM {cat}.bronze.raw_events GROUP BY source""")
