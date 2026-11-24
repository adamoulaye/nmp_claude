"""Modèle 3 : mouvements inhabituels (IsolationForest) et causalité de Granger sentiment -> volatilité."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from nmp.common.config import get_settings

log = logging.getLogger(__name__)

MIN_BARS = 2000
ANOMALY_COLS = ["hk_security", "bar_ts", "anomaly_score", "anomaly_type", "model_version"]


def _spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def news_coverage(index: pd.DatetimeIndex, news_ts: pd.Series, news_window: tuple[str, str] = ("5min", "15min")) -> np.ndarray:
    """Vrai si une nouvelle a ete publiee entre -15 min et +5 min de chaque horodatage (meme regle que detect)."""
    nts = pd.DatetimeIndex(pd.to_datetime(news_ts, utc=True)).sort_values()
    if len(nts) == 0:
        return np.zeros(len(index), bool)
    after, before = pd.Timedelta(news_window[0]), pd.Timedelta(news_window[1])
    idx = nts.searchsorted(index - before)
    return np.asarray((idx < len(nts)) & (nts[np.clip(idx, 0, len(nts) - 1)] <= index + after))


def baseline_stats(hk: str, res: pd.DataFrame, news_ts: pd.Series, version: str) -> dict:
    """Lignes de base du modele 3 pour un titre.

    base_rate_news : part de TOUTES les barres de test qui ont une nouvelle a proximite (hasard).
    explained_rate_iforest : part des anomalies de l'IsolationForest qui en ont une.
    explained_rate_zscore : meme mesure pour un detecteur de seuil (memes effectifs, plus grands z robustes).
    """
    cov = news_coverage(pd.DatetimeIndex(res.index), news_ts)
    n_an = int(res["is_anomaly"].sum())
    out = {
        "hk_security": hk,
        "model_version": version,
        "n_test_bars": int(len(res)),
        "n_anomalies": n_an,
        "base_rate_news": float(cov.mean()) if len(res) else None,
        "explained_rate_iforest": None,
        "explained_rate_zscore": None,
        "overlap_zscore": None,
    }
    if n_an:
        score = res[["z_ret", "z_vol", "z_range"]].abs().max(axis=1)
        top = score.nlargest(n_an).index
        pos = res.index.get_indexer(top)
        flagged = set(res.index[res["is_anomaly"].to_numpy()])
        out["explained_rate_iforest"] = float(res["explained_by_news"].sum() / n_an)
        out["explained_rate_zscore"] = float(cov[pos].mean())
        out["overlap_zscore"] = float(len(flagged & set(top)) / n_an)
    return out


def anomaly_rows(
    bars: pd.DataFrame, news: pd.DataFrame, version: str, min_bars: int = MIN_BARS, stats: list | None = None
) -> pd.DataFrame:
    """bars : hk_security, bar_ts, close, high, low, volume. news : hk_security, published_utc.

    Retourne uniquement les barres anormales (expliquées ou non par une nouvelle).
    """
    from nmp.ml.anomaly import detect

    b = bars.copy()
    b["bar_ts"] = pd.to_datetime(b["bar_ts"], utc=True)
    n = news.copy()
    n["published_utc"] = pd.to_datetime(n["published_utc"], utc=True)
    out = []
    for hk, g in b.groupby("hk_security"):
        g = g.drop_duplicates("bar_ts").set_index("bar_ts").sort_index()[["close", "high", "low", "volume"]]
        if len(g) < min_bars:
            log.warning("Anomalies : %s ignoré (%d barres < %d)", hk, len(g), min_bars)
            continue
        ts = n.loc[n["hk_security"] == hk, "published_utc"]
        res = detect(g, ts)
        if stats is not None:
            stats.append(baseline_stats(str(hk), res, ts, version))
        an = res[res["is_anomaly"]]
        if an.empty:
            continue
        out.append(
            pd.DataFrame(
                {
                    "hk_security": hk,
                    "bar_ts": an.index,
                    "anomaly_score": an["anomaly_score"].to_numpy(),
                    "anomaly_type": an["anomaly_type"].to_numpy(),
                    "model_version": version,
                }
            )
        )
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=ANOMALY_COLS)


def hourly_series(bars: pd.DataFrame, news: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Séries horaires (UTC) : sentiment moyen des articles et volatilité réalisée moyenne des titres.

    Les rendements qui enjambent une coupure de plus de 30 minutes (nuit, week-end) sont ignorés.
    bars : hk_security, bar_ts, close. news : published_utc, sentiment_score.
    """
    b = bars.copy()
    b["bar_ts"] = pd.to_datetime(b["bar_ts"], utc=True)
    b = b.sort_values(["hk_security", "bar_ts"])
    g = b.groupby("hk_security")
    b["r"] = g["close"].transform(lambda s: np.log(s).diff())
    gap = g["bar_ts"].diff() > pd.Timedelta("30min")
    b["r2"] = (b["r"].where(~gap)) ** 2
    b["hour"] = b["bar_ts"].dt.floor("h")
    rv = np.sqrt(b.groupby(["hk_security", "hour"])["r2"].sum(min_count=1)).groupby(level="hour").mean().rename("vol")
    n = news.copy()
    n["hour"] = pd.to_datetime(n["published_utc"], utc=True).dt.floor("h")
    sent = n.groupby("hour")["sentiment_score"].mean().rename("sent")
    return sent, rv


def anomalies() -> None:
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

    from nmp.common.lakehouse import merge_insert_only
    from nmp.common.runlog import run_log

    s = get_settings()
    cat = s.catalog
    spark = _spark()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    computed = datetime.now(timezone.utc)
    version = "iforest_" + computed.strftime("%Y%m%d%H%M")
    with run_log(spark, cat, "model", "anomalies") as st:
        bars = spark.sql(f"SELECT hk_security, bar_ts, close, high, low, volume FROM {cat}.vault.nhsat_bar_1m").toPandas()
        news = spark.sql(
            f"""SELECT l.hk_security, min(a.published_utc) AS published_utc
                FROM {cat}.vault.sat_article_content a
                JOIN {cat}.vault.link_article_security l ON a.hk_article = l.hk_article
                LEFT JOIN {cat}.business_vault.bv_article_enriched e ON a.hk_article = e.hk_article
                WHERE coalesce(e.is_first, true)
                  AND coalesce(a.source_name, '') <> 'test'
                GROUP BY l.hk_security, a.hk_article"""
        ).toPandas()
        st.received = len(bars)
        stats: list = []
        rows = anomaly_rows(bars, news, version, stats=stats)
        st.kept = len(rows)
        if len(rows):
            schema = StructType(
                [
                    StructField("hk_security", StringType(), True),
                    StructField("bar_ts", TimestampType(), True),
                    StructField("anomaly_score", DoubleType(), True),
                    StructField("anomaly_type", StringType(), True),
                    StructField("model_version", StringType(), True),
                    StructField("computed_utc", TimestampType(), True),
                ]
            )
            recs = [
                (r.hk_security, r.bar_ts.to_pydatetime(), float(r.anomaly_score), r.anomaly_type, r.model_version, computed)  # type: ignore[union-attr,arg-type]
                for r in rows.itertuples(index=False)
            ]
            merge_insert_only(
                spark,
                spark.createDataFrame(recs, schema=schema),
                f"{cat}.business_vault.bv_anomaly",
                ["hk_security", "bar_ts", "model_version"],
            )
        spark.sql(
            f"""CREATE OR REPLACE VIEW {cat}.gold.fact_anomaly AS
                SELECT a.hk_security, d.symbol, d.sector, a.bar_ts, a.anomaly_score, a.anomaly_type,
                       a.model_version, a.computed_utc
                FROM {cat}.business_vault.bv_anomaly a
                JOIN {cat}.gold.dim_security d ON a.hk_security = d.hk_security
                WHERE a.model_version = (SELECT max(model_version) FROM {cat}.business_vault.bv_anomaly)"""
        )
        spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {cat}.gold.fact_anomaly_baseline (
                  hk_security STRING, model_version STRING, n_test_bars INT, n_anomalies INT,
                  base_rate_news DOUBLE, explained_rate_iforest DOUBLE, explained_rate_zscore DOUBLE,
                  overlap_zscore DOUBLE, computed_utc TIMESTAMP)"""
        )
        if stats:
            now = datetime.now(timezone.utc)
            bschema = StructType(
                [
                    StructField("hk_security", StringType(), True),
                    StructField("model_version", StringType(), True),
                    StructField("n_test_bars", IntegerType(), True),
                    StructField("n_anomalies", IntegerType(), True),
                    StructField("base_rate_news", DoubleType(), True),
                    StructField("explained_rate_iforest", DoubleType(), True),
                    StructField("explained_rate_zscore", DoubleType(), True),
                    StructField("overlap_zscore", DoubleType(), True),
                    StructField("computed_utc", TimestampType(), True),
                ]
            )
            brecs = [
                (
                    d["hk_security"],
                    d["model_version"],
                    d["n_test_bars"],
                    d["n_anomalies"],
                    d["base_rate_news"],
                    d["explained_rate_iforest"],
                    d["explained_rate_zscore"],
                    d["overlap_zscore"],
                    now,
                )
                for d in stats
            ]
            spark.createDataFrame(brecs, schema=bschema).write.mode("append").saveAsTable(f"{cat}.gold.fact_anomaly_baseline")
        spark.sql(
            f"""CREATE OR REPLACE VIEW {cat}.gold.v_anomaly_baseline_latest AS
                SELECT d.symbol, d.sector, b.*
                FROM {cat}.gold.fact_anomaly_baseline b
                JOIN {cat}.gold.dim_security d ON b.hk_security = d.hk_security
                WHERE b.model_version = (SELECT max(model_version) FROM {cat}.gold.fact_anomaly_baseline)"""
        )
        st.message = f"{version} : {len(rows)} anomalies sur {len(bars)} barres"
        log.info(st.message)


def granger() -> None:
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

    from nmp.common.runlog import run_log
    from nmp.ml.granger import granger_pair

    s = get_settings()
    cat = s.catalog
    spark = _spark()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    now = datetime.now(timezone.utc)
    with run_log(spark, cat, "model", "granger") as st:
        bars = spark.sql(f"SELECT hk_security, bar_ts, close FROM {cat}.vault.nhsat_bar_1m").toPandas()
        news = spark.sql(
            f"""SELECT published_utc, sentiment_score FROM {cat}.business_vault.bv_article_enriched
                WHERE is_first AND sentiment_score IS NOT NULL"""
        ).toPandas()
        st.received = len(bars)
        sent, vol = hourly_series(bars, news)
        n_obs = int(pd.concat([sent, vol], axis=1).dropna().shape[0])
        res = granger_pair(sent, vol)
        st.kept = len(res)
        spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {cat}.gold.fact_granger (
                  scope STRING, cause STRING, effect STRING, lag INT,
                  p_value DOUBLE, p_bonf DOUBLE, n_obs INT, computed_utc TIMESTAMP)"""
        )
        schema = StructType(
            [
                StructField("scope", StringType(), True),
                StructField("cause", StringType(), True),
                StructField("effect", StringType(), True),
                StructField("lag", IntegerType(), True),
                StructField("p_value", DoubleType(), True),
                StructField("p_bonf", DoubleType(), True),
                StructField("n_obs", IntegerType(), True),
                StructField("computed_utc", TimestampType(), True),
            ]
        )
        recs = [
            ("tous titres, horaire", r.cause, r.effect, int(r.lag), float(r.p_value), float(r.p_bonf), n_obs, now)  # type: ignore[arg-type]
            for r in res.itertuples(index=False)
        ]
        spark.createDataFrame(recs, schema=schema).write.mode("append").saveAsTable(f"{cat}.gold.fact_granger")
        spark.sql(
            f"""CREATE OR REPLACE VIEW {cat}.gold.v_granger_latest AS
                SELECT * FROM {cat}.gold.fact_granger
                WHERE computed_utc = (SELECT max(computed_utc) FROM {cat}.gold.fact_granger)"""
        )
        st.message = f"Granger : {len(res)} tests sur {n_obs} heures"
        log.info(st.message)
