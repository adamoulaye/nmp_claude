"""Écritures Delta idempotentes + points de contrôle (watermarks)."""

from __future__ import annotations

from datetime import datetime, timezone


def merge_insert_only(spark, df, table: str, keys: list[str]) -> None:
    """MERGE insert-only : rejouer le même lot ne crée aucun doublon (idempotence)."""
    from delta.tables import DeltaTable

    cond = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(df.dropDuplicates(keys).alias("s"), cond)
        .whenNotMatchedInsertAll()
        .execute()
    )


def get_watermark(spark, catalog: str, source: str, default: datetime) -> datetime:
    rows = spark.sql(f"SELECT watermark_utc FROM {catalog}.ops.watermarks WHERE source = '{source}'").collect()
    return rows[0]["watermark_utc"] if rows else default


def set_watermark(spark, catalog: str, source: str, ts: datetime) -> None:
    spark.sql(
        f"""MERGE INTO {catalog}.ops.watermarks t
            USING (SELECT '{source}' AS source, TIMESTAMP'{ts.astimezone(timezone.utc).isoformat()}' AS watermark_utc) s
            ON t.source = s.source
            WHEN MATCHED THEN UPDATE SET watermark_utc = s.watermark_utc
            WHEN NOT MATCHED THEN INSERT *"""
    )
