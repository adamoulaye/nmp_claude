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


def backfill_finnhub_news(start: str, end: str, pause: float = 45.0) -> None:
    """Recharge les actualites Finnhub jour par jour (dates AAAA-MM-JJ incluses).

    Un appel par titre et par jour evite le plafond d'environ 250 articles par appel.
    N'ecrit que dans Bronze (insertion sans doublon) et ne modifie pas le watermark.
    """
    import json
    import time
    from collections import Counter
    from datetime import date

    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    from nmp.common.lakehouse import merge_insert_only
    from nmp.common.retry import SchemaDrift
    from nmp.common.runlog import run_log
    from nmp.ingestion.base import payload_hash, schema_fingerprint
    from nmp.ingestion.sources import FinnhubNews

    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    if d1 < d0:
        raise ValueError("--end doit etre posterieur ou egal a --start.")

    s = get_settings()
    spark = _spark()
    src = FinnhubNews(s)
    cat = s.catalog
    schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("record_id", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("published_utc", TimestampType(), True),
            StructField("updated_utc", TimestampType(), True),
            StructField("ingested_utc", TimestampType(), True),
            StructField("payload", StringType(), True),
            StructField("record_hash", StringType(), True),
            StructField("schema_fp", StringType(), True),
        ]
    )

    with run_log(spark, cat, "backfill", src.name) as st:
        total_kept = total_bad = total_recv = 0
        d = d0
        while d <= d1:
            day = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            good, bad, per_symbol = [], [], Counter()
            for raw in src.fetch(day, day):
                total_recv += 1
                try:
                    src.check_schema(raw)
                    rec = src.to_record(raw)
                except SchemaDrift:
                    raise
                except Exception as exc:
                    bad.append({"source": src.name, "raw": json.dumps(raw, default=str)[:20000], "error": str(exc)[:500]})
                    continue
                if rec is None or rec.record_id is None or rec.published_utc is None:
                    bad.append(
                        {"source": src.name, "raw": json.dumps(raw, default=str)[:20000], "error": "id/horodatage manquant"}
                    )
                    continue
                per_symbol[rec.symbol] += 1
                good.append(
                    {
                        "source": rec.source,
                        "record_id": str(rec.record_id),
                        "symbol": rec.symbol,
                        "published_utc": rec.published_utc,
                        "updated_utc": rec.updated_utc,
                        "ingested_utc": datetime.now(timezone.utc),
                        "payload": json.dumps(rec.payload, default=str),
                        "record_hash": payload_hash(rec.payload),
                        "schema_fp": schema_fingerprint(rec.payload),
                    }
                )
            capped = sorted(k for k, v in per_symbol.items() if v >= 240)
            if capped:
                log.warning("Finnhub %s : plafond probablement atteint pour %s", d.isoformat(), capped)
            if good:
                merge_insert_only(
                    spark,
                    spark.createDataFrame(good, schema=schema),
                    f"{cat}.{src.bronze_table}",
                    ["source", "record_id", "record_hash"],
                )
            if bad:
                spark.createDataFrame(bad).write.mode("append").saveAsTable(f"{cat}.bronze.rejects")
            total_kept += len(good)
            total_bad += len(bad)
            log.info("Finnhub %s : %d articles, %d rejetes", d.isoformat(), len(good), len(bad))
            d += timedelta(days=1)
            if d <= d1:
                time.sleep(pause)
        st.received, st.kept, st.rejected = total_recv, total_kept, total_bad
        st.message = f"Backfill Finnhub {d0.isoformat()} -> {d1.isoformat()} ; {total_kept} gardes, {total_bad} rejetes"


def backfill_bars(start: str, end: str) -> None:
    """Recharge une période historique de barres sans modifier le watermark."""
    import json

    from pyspark.sql.types import (
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    from nmp.common.lakehouse import merge_insert_only
    from nmp.common.retry import SchemaDrift
    from nmp.common.runlog import run_log
    from nmp.ingestion.base import payload_hash, schema_fingerprint
    from nmp.ingestion.sources import AlpacaBars1m

    def parse_utc(value: str) -> datetime:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("Les dates doivent inclure un fuseau horaire, par exemple Z.")
        return dt.astimezone(timezone.utc)

    start_utc = parse_utc(start)
    end_utc = parse_utc(end)

    if end_utc <= start_utc:
        raise ValueError("--end doit être postérieur à --start.")

    s = get_settings()
    spark = _spark()
    src = AlpacaBars1m(s)
    cat = s.catalog

    good, bad = [], []

    with run_log(spark, cat, "backfill", src.name) as st:
        for raw in src.fetch(start_utc, end_utc):
            st.received += 1

            try:
                src.check_schema(raw)
                rec = src.to_record(raw)
            except SchemaDrift:
                raise
            except Exception as exc:
                bad.append(
                    {
                        "source": src.name,
                        "raw": json.dumps(raw, default=str)[:20000],
                        "error": str(exc)[:500],
                    }
                )
                continue

            if rec is None or rec.record_id is None or rec.published_utc is None:
                bad.append(
                    {
                        "source": src.name,
                        "raw": json.dumps(raw, default=str)[:20000],
                        "error": "id/horodatage manquant",
                    }
                )
                continue

            good.append(
                {
                    "source": rec.source,
                    "record_id": str(rec.record_id),
                    "symbol": rec.symbol,
                    "published_utc": rec.published_utc,
                    "updated_utc": rec.updated_utc,
                    "ingested_utc": datetime.now(timezone.utc),
                    "payload": json.dumps(rec.payload, default=str),
                    "record_hash": payload_hash(rec.payload),
                    "schema_fp": schema_fingerprint(rec.payload),
                }
            )

        st.kept = len(good)
        st.rejected = len(bad)

        if good:
            schema = StructType(
                [
                    StructField("source", StringType(), True),
                    StructField("record_id", StringType(), True),
                    StructField("symbol", StringType(), True),
                    StructField("published_utc", TimestampType(), True),
                    StructField("updated_utc", TimestampType(), True),
                    StructField("ingested_utc", TimestampType(), True),
                    StructField("payload", StringType(), True),
                    StructField("record_hash", StringType(), True),
                    StructField("schema_fp", StringType(), True),
                ]
            )

            df = spark.createDataFrame(good, schema=schema)

            merge_insert_only(
                spark,
                df,
                f"{cat}.{src.bronze_table}",
                ["source", "record_id", "record_hash"],
            )

        if bad:
            spark.createDataFrame(bad).write.mode("append").saveAsTable(f"{cat}.bronze.rejects")

        st.message = (
            f"Backfill {start_utc.isoformat()} -> {end_utc.isoformat()} ; "
            f"{len(good)} reçues et valides, {len(bad)} rejetées. "
            "Watermark inchangé."
        )

    log.info(
        "Backfill terminé : %s enregistrements valides, %s rejetés. Watermark inchangé.",
        len(good),
        len(bad),
    )


def enrich(lookback_hours: int = 72) -> None:
    """Silver/Vault -> Business Vault : dédoublonnage, séance, sentiment et catégorie."""
    import pandas as pd

    from nmp.common.runlog import run_log
    from nmp.common.timeutil import attach_session, build_sessions
    from nmp.ml.sentiment import FinBertScorer
    from nmp.transform.category import categorize
    from nmp.transform.dedup import cluster_articles

    s, spark = get_settings(), _spark()
    cat = s.catalog
    model_version = "finbert-prosus-v1"

    with run_log(spark, cat, "enrich") as st:
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=lookback_hours)

        pdf = spark.sql(f"""
            SELECT
                hk_article,
                headline,
                summary,
                published_utc,
                updated_utc,
                first_seen_utc
            FROM {cat}.vault.v_article_latest
            WHERE published_utc >= TIMESTAMP'{since.isoformat()}'
        """).toPandas()

        st.received = len(pdf)

        if pdf.empty:
            log.info("Aucun article dans la fenêtre de traitement.")
            return

        pdf["text"] = pdf["headline"].fillna("") + ". " + pdf["summary"].fillna("")

        d = cluster_articles(pdf)

        sessions = build_sessions(
            (since - timedelta(days=10)).date().isoformat(),
            (now + timedelta(days=14)).date().isoformat(),
        )

        d = attach_session(d, sessions)

        d["available_utc"] = pd.concat(
            [
                pd.to_datetime(d["published_utc"], utc=True),
                pd.to_datetime(d["first_seen_utc"], utc=True),
            ],
            axis=1,
        ).max(axis=1)

        d["category"] = [
            categorize(headline, summary)
            for headline, summary in zip(
                d["headline"].fillna(""),
                d["summary"].fillna(""),
                strict=True,
            )
        ]

        candidates = d[["hk_article"]].drop_duplicates()
        spark.createDataFrame(candidates).createOrReplaceTempView("_enrich_candidates")

        pending = spark.sql(f"""
            SELECT c.hk_article
            FROM _enrich_candidates c
            LEFT ANTI JOIN {cat}.business_vault.bv_article_enriched t
                ON t.hk_article = c.hk_article
                AND t.model_version = '{model_version}'
        """).toPandas()

        pending_ids = set(pending["hk_article"].tolist())

        if not pending_ids:
            log.info("Tous les articles récents sont déjà enrichis.")
            return

        d = d[d["hk_article"].isin(pending_ids)].copy()
        d = d.reset_index(drop=True)

        scorer = FinBertScorer()
        scores = scorer.score(d["text"].tolist())

        d = pd.concat([d, scores], axis=1)

        d["model_version"] = model_version
        d["load_ts"] = pd.Timestamp.now(tz="UTC")

        st.kept = len(d)

        output_columns = [
            "hk_article",
            "published_utc",
            "updated_utc",
            "first_seen_utc",
            "available_utc",
            "cluster_id",
            "is_first",
            "dup_rank",
            "cluster_size",
            "session_type",
            "session_date",
            "reaction_start_utc",
            "positive",
            "negative",
            "neutral",
            "sentiment_score",
            "sentiment_label",
            "category",
            "model_version",
            "load_ts",
        ]

        out = d[output_columns].copy()

        timestamp_columns = [
            "published_utc",
            "updated_utc",
            "first_seen_utc",
            "available_utc",
            "reaction_start_utc",
            "load_ts",
        ]

        for col in timestamp_columns:
            out[col] = pd.to_datetime(out[col], utc=True).dt.tz_localize(None)

        spark.createDataFrame(out).createOrReplaceTempView("_enriched")

        spark.sql(f"""
            MERGE INTO {cat}.business_vault.bv_article_enriched t
            USING _enriched s
            ON t.hk_article = s.hk_article
               AND t.model_version = s.model_version

            WHEN NOT MATCHED THEN INSERT (
                hk_article,
                published_utc,
                updated_utc,
                first_seen_utc,
                available_utc,
                cluster_id,
                is_first,
                dup_rank,
                cluster_size,
                session_type,
                session_date,
                reaction_start_utc,
                positive,
                negative,
                neutral,
                sentiment_score,
                sentiment_label,
                category,
                model_version,
                load_ts
            )
            VALUES (
                s.hk_article,
                s.published_utc,
                s.updated_utc,
                s.first_seen_utc,
                s.available_utc,
                s.cluster_id,
                s.is_first,
                s.dup_rank,
                s.cluster_size,
                s.session_type,
                s.session_date,
                s.reaction_start_utc,
                s.positive,
                s.negative,
                s.neutral,
                s.sentiment_score,
                s.sentiment_label,
                s.category,
                s.model_version,
                s.load_ts
            )
        """)

        log.info(
            "Enrichissement terminé : %d articles traités.",
            len(out),
        )


def car(lookback_hours: int = 192) -> None:
    """Calcule les CAR selon deux références temporelles.

    published : réaction à la publication de l'article.
    available : réaction à la disponibilité de l'article sur la plateforme.

    Fenêtres [1,15] et [1,60] minutes.
    Les événements sont traités uniquement si les 60 minutes
    de séance nécessaires sont disponibles avant la fermeture NYSE.
    """
    import pandas as pd

    from nmp import __version__ as CODE_VERSION
    from nmp.common.runlog import run_log
    from nmp.common.timeutil import build_sessions
    from nmp.transform.car_batch import run_batch, to_bv_event_car

    s, spark = get_settings(), _spark()
    cat = s.catalog

    with run_log(spark, cat, "car") as st:
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=lookback_hours)
        until = now - timedelta(minutes=65)

        model_version = f"market_model_{CODE_VERSION}_sip"

        events = spark.sql(f"""
            WITH candidates AS (

                SELECT
                    e.hk_article,
                    l.hk_security,
                    h.bk_symbol AS ticker,
                    'published' AS event_time_basis,
                    e.published_utc AS event_utc
                FROM {cat}.business_vault.bv_article_enriched e
                JOIN {cat}.vault.link_article_security l
                  USING (hk_article)
                JOIN {cat}.vault.hub_security h
                  USING (hk_security)
                WHERE e.is_first
                  AND e.session_type = 'pendant'

                UNION ALL

                SELECT
                    e.hk_article,
                    l.hk_security,
                    h.bk_symbol AS ticker,
                    'available' AS event_time_basis,
                    e.available_utc AS event_utc
                FROM {cat}.business_vault.bv_article_enriched e
                JOIN {cat}.vault.link_article_security l
                  USING (hk_article)
                JOIN {cat}.vault.hub_security h
                  USING (hk_security)
                WHERE e.is_first
                  AND e.available_utc IS NOT NULL
            ),

            eligible AS (
                SELECT *
                FROM candidates
                WHERE event_utc BETWEEN
                    TIMESTAMP'{since.isoformat()}'
                    AND TIMESTAMP'{until.isoformat()}'
                  AND dayofweek(
                    to_date(from_utc_timestamp(
                        event_utc, 'America/New_York'
                    ))
                  ) BETWEEN 2 AND 6
                  AND (
                    hour(from_utc_timestamp(
                        event_utc, 'America/New_York'
                    )) * 60
                    +
                    minute(from_utc_timestamp(
                        event_utc, 'America/New_York'
                    ))
                  ) BETWEEN 570 AND 900
            )

            SELECT e.*
            FROM eligible e
            WHERE
                (
                    NOT EXISTS (
                        SELECT 1
                        FROM {cat}.business_vault.bv_event_car c15
                        WHERE c15.hk_article = e.hk_article
                          AND c15.hk_security = e.hk_security
                          AND coalesce(c15.event_time_basis, 'published')
                              = e.event_time_basis
                          AND c15.window_end_min = 15
                          AND c15.model_version = '{model_version}'
                    )
                    OR
                    NOT EXISTS (
                        SELECT 1
                        FROM {cat}.business_vault.bv_event_car c60
                        WHERE c60.hk_article = e.hk_article
                          AND c60.hk_security = e.hk_security
                          AND coalesce(c60.event_time_basis, 'published')
                              = e.event_time_basis
                          AND c60.window_end_min = 60
                          AND c60.model_version = '{model_version}'
                    )
                )
        """).toPandas()

        st.received = len(events)

        if events.empty:
            log.info("Aucun événement CAR à calculer.")
            return

        events["event_utc"] = pd.to_datetime(
            events["event_utc"],
            utc=True,
        )

        # ---------------------------------------------------------
        # Vérification du calendrier officiel NYSE.
        # Cela élimine les jours fériés et tient compte des
        # séances écourtées.
        # ---------------------------------------------------------
        min_local_date = events["event_utc"].dt.tz_convert("America/New_York").dt.date.min()
        max_local_date = events["event_utc"].dt.tz_convert("America/New_York").dt.date.max()

        session_start = (pd.Timestamp(min_local_date) - pd.Timedelta(days=2)).date().isoformat()

        session_end = (pd.Timestamp(max_local_date) + pd.Timedelta(days=2)).date().isoformat()

        sessions = build_sessions(session_start, session_end)

        if sessions.empty:
            log.warning("Aucune séance NYSE trouvée pour les événements.")
            st.rejected = len(events)
            return

        sessions = sessions.copy()
        sessions["session_date"] = pd.to_datetime(sessions["session_date"]).dt.date

        events["_session_date"] = events["event_utc"].dt.tz_convert("America/New_York").dt.date

        events = events.merge(
            sessions[["session_date", "open_utc", "close_utc"]],
            left_on="_session_date",
            right_on="session_date",
            how="inner",
        )

        if events.empty:
            log.info("Aucun événement ne correspond à une séance officielle NYSE.")
            return

        events["open_utc"] = pd.to_datetime(
            events["open_utc"],
            utc=True,
        )
        events["close_utc"] = pd.to_datetime(
            events["close_utc"],
            utc=True,
        )

        # Les barres sont horodatées au début de chaque minute.
        # Pour disposer de 60 rendements après m0, la dernière
        # barre nécessaire est celle de m0 + 60 minutes.
        events["_m0"] = events["event_utc"].dt.floor("min")
        events["_last_required_bar"] = events["_m0"] + pd.Timedelta(minutes=60)
        events["_last_session_bar"] = events["close_utc"] - pd.Timedelta(minutes=1)

        eligible_mask = (events["event_utc"] >= events["open_utc"]) & (
            events["_last_required_bar"] <= events["_last_session_bar"]
        )

        excluded_session_count = int((~eligible_mask).sum())

        if excluded_session_count:
            log.info(
                "CAR : %d événement(s) écarté(s), car la fenêtre de 60 minutes dépasse la séance NYSE.",
                excluded_session_count,
            )

        events = events.loc[eligible_mask].copy()

        events = events.drop(
            columns=[
                "_session_date",
                "session_date",
                "open_utc",
                "close_utc",
                "_m0",
                "_last_required_bar",
                "_last_session_bar",
            ],
            errors="ignore",
        )

        if events.empty:
            log.info("Aucun événement ne dispose de 60 minutes complètes avant la fermeture NYSE.")
            return

        # Dix jours calendaires pour couvrir les week-ends
        # et disposer d'une fenêtre d'estimation suffisante.
        lo = events["event_utc"].min() - timedelta(days=10)
        hi = events["event_utc"].max() + timedelta(hours=2)

        tickers = set(events["ticker"].dropna().tolist())
        tickers.add(s.benchmark)

        syms = ", ".join("'" + str(t).replace("'", "''") + "'" for t in sorted(tickers))

        bars = spark.sql(f"""
            SELECT
                h.bk_symbol AS ticker,
                b.bar_ts AS ts_utc,
                b.close,
                b.volume
            FROM {cat}.gold.fact_bar_1m b
            JOIN {cat}.vault.hub_security h
              USING (hk_security)
            WHERE h.bk_symbol IN ({syms})
              AND b.bar_ts BETWEEN
                  TIMESTAMP'{lo.isoformat()}'
                  AND TIMESTAMP'{hi.isoformat()}'
        """).toPandas()

        if bars.empty:
            log.warning("Aucun cours boursier disponible pour les événements CAR.")
            st.rejected = len(events)
            return

        bars["ts_utc"] = pd.to_datetime(
            bars["ts_utc"],
            utc=True,
        )

        res, _, err = run_batch(
            events,
            bars,
            s.benchmark,
            id_col="hk_article",
            ts_col="event_utc",
            keep=("hk_security", "event_time_basis"),
        )

        # ---------------------------------------------------------
        # Bilan du traitement : calculés, en attente et erreurs.
        # Les événements pending ne sont pas enregistrés.
        # Ils seront repris au prochain batch.
        # ---------------------------------------------------------
        st.kept = len(res)

        pending_count = 0
        error_count = 0

        if not err.empty:
            if "status" in err.columns:
                pending_count = int((err["status"] == "pending").sum())
                error_count = int((err["status"] == "error").sum())
            else:
                error_count = len(err)

        st.rejected = error_count

        log.info(
            "CAR : %d événement(s) calculé(s), %d en attente de données complètes, %d erreur(s).",
            st.kept,
            pending_count,
            error_count,
        )

        if error_count > 0:
            if "status" in err.columns:
                error_rows = err[err["status"] == "error"]
            else:
                error_rows = err

            for _, row in error_rows.head(5).iterrows():
                log.warning(
                    "CAR : erreur pour %s : %s",
                    row.get("id", "inconnu"),
                    row.get("error", "Erreur non précisée"),
                )

        if pending_count > 0:
            log.info("Les événements incomplets ne sont pas enregistrés et pourront être retraités au prochain batch.")

        if res.empty:
            log.info("Aucun résultat CAR exploitable.")
            return

        out = to_bv_event_car(res)
        out["model_version"] = model_version

        out["load_ts"] = pd.to_datetime(out["load_ts"], utc=True).dt.tz_localize(None)

        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("hk_article", StringType(), True),
                StructField("hk_security", StringType(), True),
                StructField("event_time_basis", StringType(), True),
                StructField("window_start_min", IntegerType(), True),
                StructField("window_end_min", IntegerType(), True),
                StructField("car", DoubleType(), True),
                StructField("t_stat", DoubleType(), True),
                StructField("p_value", DoubleType(), True),
                StructField("alpha", DoubleType(), True),
                StructField("beta", DoubleType(), True),
                StructField("n_est", IntegerType(), True),
                StructField("car_pre", DoubleType(), True),
                StructField("leak_flag", BooleanType(), True),
                StructField("model_version", StringType(), True),
                StructField("load_ts", TimestampType(), True),
            ]
        )

        columns = [field.name for field in schema.fields]
        out = out[columns].copy()
        out = out.astype(object).where(pd.notna(out), None)

        records = out.to_dict(orient="records")

        for record in records:
            value = record["load_ts"]
            if value is not None and isinstance(value, pd.Timestamp):
                record["load_ts"] = value.to_pydatetime()

        spark.createDataFrame(
            records,
            schema=schema,
        ).createOrReplaceTempView("_car")

        spark.sql(f"""
            MERGE INTO {cat}.business_vault.bv_event_car AS t
            USING _car AS s
            ON t.hk_article = s.hk_article
               AND t.hk_security = s.hk_security
               AND coalesce(t.event_time_basis, 'published')
                   = s.event_time_basis
               AND t.window_end_min = s.window_end_min
               AND t.model_version = s.model_version

            WHEN NOT MATCHED THEN INSERT (
                hk_article,
                hk_security,
                event_time_basis,
                window_start_min,
                window_end_min,
                car,
                t_stat,
                p_value,
                alpha,
                beta,
                n_est,
                car_pre,
                leak_flag,
                model_version,
                load_ts
            )
            VALUES (
                s.hk_article,
                s.hk_security,
                s.event_time_basis,
                s.window_start_min,
                s.window_end_min,
                s.car,
                s.t_stat,
                s.p_value,
                s.alpha,
                s.beta,
                s.n_est,
                s.car_pre,
                s.leak_flag,
                s.model_version,
                s.load_ts
            )
        """)

        log.info(
            "CAR terminé : %d lignes enregistrées, %d événements en attente, %d erreurs.",
            len(out),
            pending_count,
            error_count,
        )


def train(experiment: str = "/Shared/nmp/event_impact") -> None:
    from nmp.ml.event_classifier import train_and_log

    spark = _spark()
    df = spark.table(f"{get_settings().catalog}.gold.v_training_set").toPandas()

    res = train_and_log(
        df,
        experiment,
        f"{get_settings().catalog}.gold.event_magnitude",
    )

    log.info(res)


def health() -> None:
    """Contrôles de fraîcheur -> table ops.freshness."""
    cat = get_settings().catalog

    _spark().sql(f"""
        INSERT INTO {cat}.ops.freshness
        SELECT
            current_timestamp(),
            source,
            max(published_utc),
            max(ingested_utc),
            (
                unix_timestamp(current_timestamp())
                - unix_timestamp(max(ingested_utc))
            ) / 60.0
        FROM {cat}.bronze.raw_events
        GROUP BY source
    """)


def publish() -> None:
    """Publie métriques, prédictions et dérive des modèles vers le Gold."""
    import mlflow
    import numpy as np
    import pandas as pd
    from mlflow.tracking import MlflowClient
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    from nmp.common.runlog import run_log
    from nmp.ml.drift import psi
    from nmp.ml.event_classifier import CAT_FEATURES, NUM_FEATURES, prepare

    cat = get_settings().catalog
    spark = _spark()
    mlflow.set_registry_uri("databricks-uc")
    client = MlflowClient()

    base = f"{cat}.gold.event_magnitude"
    names = ["baseline_prior", "baseline_logreg", "xgboost", "random_forest"]
    x_cols = CAT_FEATURES + NUM_FEATURES

    with run_log(spark, cat, "publish") as st:
        # 1. Dernière version de chaque modèle : même train, même jeu, même découpage
        chosen = {}
        for n in names:
            full = f"{base}_{n}"
            versions = client.search_model_versions(f"name='{full}'")
            if not versions:
                log.warning("Modèle absent du registre : %s", full)
                continue
            mv = max(versions, key=lambda v: int(v.version))
            chosen[n] = (full, mv, client.get_run(mv.run_id).data.metrics)

        # Champion = meilleur holdout_macro_f1 parmi les modèles d'arbres (Random Forest,
        # XGBoost). Les baselines (prior, logreg) restent les lignes de base de comparaison.
        candidates = {n: v for n, v in chosen.items() if not n.startswith("baseline")}
        if not candidates:
            raise ValueError("Aucun modèle non-baseline dans le registre : lancer d'abord train.")
        champion = max(
            candidates,
            key=lambda n: candidates[n][2].get("holdout_macro_f1", float("-inf")),
        )

        # Alias explicites (le champion ne dépend plus d'anciennes versions)
        for n, (full, mv, _m) in chosen.items():
            if n.startswith("baseline"):
                client.set_registered_model_alias(full, "baseline", mv.version)
                for stale in ("champion", "challenger"):
                    try:
                        client.delete_registered_model_alias(full, stale)
                    except Exception:  # noqa: S110
                        pass  # alias absent
                continue
            target, other = ("champion", "challenger") if n == champion else ("challenger", "champion")
            client.set_registered_model_alias(full, target, mv.version)
            try:
                client.delete_registered_model_alias(full, other)
            except Exception:  # noqa: S110
                pass  # alias absent

        now = datetime.now(timezone.utc).replace(tzinfo=None)

        # 2. Métriques -> gold.fact_model_metrics (idempotent)
        metric_rows = []
        for n, (_full, mv, metrics) in chosen.items():
            if n == champion:
                alias = "champion"
            elif n.startswith("baseline"):
                alias = "baseline"
            else:
                alias = "challenger"
            for metric, value in metrics.items():
                metric_rows.append((mv.run_id, f"event_magnitude_{n}", str(mv.version), alias, metric, float(value), now))

        m_schema = StructType(
            [
                StructField("run_id", StringType()),
                StructField("model_name", StringType()),
                StructField("model_version", StringType()),
                StructField("alias", StringType()),
                StructField("metric", StringType()),
                StructField("value", DoubleType()),
                StructField("logged_utc", TimestampType()),
            ]
        )
        spark.createDataFrame(metric_rows, schema=m_schema).createOrReplaceTempView("_pub_metrics")
        spark.sql(f"""
            MERGE INTO {cat}.gold.fact_model_metrics AS t
            USING _pub_metrics AS s
            ON t.run_id = s.run_id AND t.metric = s.metric AND t.alias = s.alias
            WHEN NOT MATCHED THEN INSERT *
        """)

        # 3. Jeu d'événements (même préparation et même découpage que l'entraînement)
        df = spark.table(f"{cat}.gold.v_training_set").toPandas()
        d = prepare(df.sort_values("published_utc").reset_index(drop=True))
        d = d.replace([np.inf, -np.inf], np.nan)
        d = d.dropna(subset=["published_utc", "abs_car"] + x_cols).reset_index(drop=True)
        st.received = len(d)

        # 4. Prédictions du champion sur le HOLDOUT uniquement (hors échantillon)
        holdout = 0.2  # identique à event_classifier.train_and_log
        cut = int(len(d) * (1 - holdout))
        te = d.iloc[cut:].reset_index(drop=True)
        full, mv, _ = chosen[champion]
        n_test_run = int(client.get_run(mv.run_id).data.params.get("n_test", -1))
        if n_test_run != len(te):
            raise ValueError(
                f"Découpage différent de l'entraînement : holdout publié {len(te)} vs n_test={n_test_run}. Publication annulée."
            )
        model = mlflow.sklearn.load_model(f"models:/{full}/{mv.version}")
        pred = model.predict(te[x_cols]).astype(int)
        proba_high = model.predict_proba(te[x_cols])[:, 2]
        out = pd.DataFrame(
            {
                "hk_article": te["hk_article"].astype(str),
                "hk_security": te["hk_security"].astype(str),
                "predicted_magnitude": pred,
                "predicted_proba_high": proba_high.astype(float),
                "model_name": f"event_magnitude_{champion}",
                "model_version": str(mv.version),
                "scored_utc": now,
            }
        )
        p_schema = StructType(
            [
                StructField("hk_article", StringType()),
                StructField("hk_security", StringType()),
                StructField("predicted_magnitude", IntegerType()),
                StructField("predicted_proba_high", DoubleType()),
                StructField("model_name", StringType()),
                StructField("model_version", StringType()),
                StructField("scored_utc", TimestampType()),
            ]
        )
        recs = [
            (
                r.hk_article,
                r.hk_security,
                int(r.predicted_magnitude),
                float(r.predicted_proba_high),
                r.model_name,
                r.model_version,
                r.scored_utc.to_pydatetime(),
            )
            for r in out.itertuples(index=False)
        ]
        spark.createDataFrame(recs, schema=p_schema).createOrReplaceTempView("_pub_pred")
        spark.sql(f"""
            MERGE INTO {cat}.gold.model_predictions AS t
            USING _pub_pred AS s
            ON t.hk_article = s.hk_article AND t.hk_security = s.hk_security
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        st.kept = len(recs)

        # 5. Dérive (PSI) : 80 % les plus anciens vs 20 % les plus récents -> gold.fact_drift
        # cut (80 % / 20 %) défini plus haut
        ref, cur = d.iloc[:cut], d.iloc[cut:]
        drift_rows = []
        for f in x_cols:
            try:
                drift_rows.append((now, f"event_magnitude_{champion}", f, float(psi(ref[f], cur[f]))))
            except Exception as exc:  # variable constante ou trop peu de valeurs
                log.warning("PSI non calculable pour %s : %s", f, exc)
        if drift_rows:
            d_schema = StructType(
                [
                    StructField("checked_utc", TimestampType()),
                    StructField("model_name", StringType()),
                    StructField("feature", StringType()),
                    StructField("psi", DoubleType()),
                ]
            )
            spark.createDataFrame(drift_rows, schema=d_schema).write.mode("append").saveAsTable(f"{cat}.gold.fact_drift")

        log.info(
            "Publication terminée : champion=%s v%s, %d prédictions (holdout), %d métriques, %d PSI.",
            champion,
            mv.version,
            len(recs),
            len(metric_rows),
            len(drift_rows),
        )
