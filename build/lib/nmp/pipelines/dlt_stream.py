"""Lakeflow Declarative Pipeline : Kafka -> Bronze (streaming) -> Silver (typé + contrôles qualité)."""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType

BOOT = spark.conf.get("kafka.bootstrap")  # noqa: F821
SCOPE = spark.conf.get("secret.scope", "nmp")  # noqa: F821


def _kafka(topic: str):
    r = (
        spark.readStream.format("kafka")  # noqa: F821
        .option("kafka.bootstrap.servers", BOOT)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
    )
    if spark.conf.get("kafka.sasl", "false") == "true":  # noqa: F821
        pw = dbutils.secrets.get(SCOPE, "KAFKA_SASL_PASSWORD")  # noqa: F821
        r = (
            r.option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option(
                "kafka.sasl.jaas.config",
                f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{pw}";',
            )
        )
    return r.load()


NEWS_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("headline", StringType()),
        StructField("summary", StringType()),
        StructField("author", StringType()),
        StructField("source", StringType()),
        StructField("url", StringType()),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
        StructField("symbols", ArrayType(StringType())),
        StructField("_received_utc", DoubleType()),
    ]
)
BAR_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("timestamp", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", DoubleType()),
        StructField("vwap", DoubleType()),
        StructField("trade_count", DoubleType()),
    ]
)


@dlt.table(name="bronze_stream_news", comment="Nouvelles Alpaca/Benzinga brutes (Kafka)", cluster_by=["ingest_ts"])
def bronze_stream_news():
    return _kafka("raw.news").select(
        F.col("key").cast("string").alias("k"),
        F.col("value").cast("string").alias("payload"),
        F.col("timestamp").alias("ingest_ts"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
    )


@dlt.table(name="bronze_stream_bars", comment="Barres 1 min brutes (Kafka)", cluster_by=["ingest_ts"])
def bronze_stream_bars():
    return _kafka("raw.bars").select(
        F.col("value").cast("string").alias("payload"),
        F.col("timestamp").alias("ingest_ts"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
    )


@dlt.table(
    name="silver_news",
    comment="Nouvelles typées, UTC, 1 ligne par (article) dédoublonnée sur id",
    cluster_by=["published_utc"],
)
@dlt.expect_or_drop("id_present", "article_id IS NOT NULL")
@dlt.expect_or_drop("headline_present", "headline IS NOT NULL AND length(trim(headline)) > 0")
@dlt.expect_or_drop("ts_valid", "published_utc IS NOT NULL AND published_utc <= current_timestamp() + INTERVAL 5 MINUTES")
@dlt.expect("has_symbols", "size(symbols) > 0")
def silver_news():
    j = F.from_json("payload", NEWS_SCHEMA)
    return (
        dlt.read_stream("bronze_stream_news")
        .select(j.alias("j"), "ingest_ts", "topic", "partition", "offset", "payload")
        .select(
            F.col("j.id").alias("article_id"),
            F.col("j.headline").alias("headline"),
            F.col("j.summary").alias("summary"),
            F.col("j.source").alias("source_name"),
            F.col("j.url").alias("url"),
            F.col("j.symbols").alias("symbols"),
            F.to_timestamp("j.created_at").alias("published_utc"),
            F.to_timestamp("j.updated_at").alias("updated_utc"),
            F.to_timestamp(F.col("j._received_utc").cast("timestamp")).alias("first_seen_utc"),
            F.col("ingest_ts").alias("ingested_utc"),
            F.sha2(F.col("payload"), 256).alias("record_hash"),
            F.concat_ws(":", F.col("topic"), F.col("partition"), F.col("offset")).alias("bronze_ref"),
        )
        .withWatermark("ingested_utc", "1 hour")
        .dropDuplicates(["article_id", "updated_utc"])
    )  # 1 version par mise à jour


@dlt.table(name="silver_bars_1m", comment="Barres 1 min brutes typées", cluster_by=["symbol", "bar_ts"])
@dlt.expect_or_drop("price_positive", "close > 0 AND open > 0 AND high >= low")
@dlt.expect_or_drop("volume_nonneg", "volume >= 0")
def silver_bars_1m():
    j = F.from_json("payload", BAR_SCHEMA)
    return (
        dlt.read_stream("bronze_stream_bars")
        .select(j.alias("j"), "ingest_ts")
        .select(
            "j.symbol",
            F.to_timestamp("j.timestamp").alias("bar_ts"),
            "j.open",
            "j.high",
            "j.low",
            "j.close",
            "j.volume",
            "j.vwap",
            "j.trade_count",
            F.col("ingest_ts").alias("ingested_utc"),
        )
        .withWatermark("bar_ts", "10 minutes")
        .dropDuplicates(["symbol", "bar_ts"])
    )
