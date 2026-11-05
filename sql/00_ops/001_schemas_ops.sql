-- Paramètre : :catalog   (ex. nmp_dev)
USE CATALOG IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS bronze; CREATE SCHEMA IF NOT EXISTS silver; CREATE SCHEMA IF NOT EXISTS vault;
CREATE SCHEMA IF NOT EXISTS business_vault; CREATE SCHEMA IF NOT EXISTS gold; CREATE SCHEMA IF NOT EXISTS ops;

-- Bronze : append-only, versionné par record_hash ; idempotent via MERGE (source, record_id, record_hash)
CREATE TABLE IF NOT EXISTS bronze.raw_events (
  source STRING, record_id STRING, symbol STRING, published_utc TIMESTAMP, updated_utc TIMESTAMP,
  ingested_utc TIMESTAMP, payload STRING, record_hash STRING, schema_fp STRING)
  CLUSTER BY (source, published_utc) TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.deletedFileRetentionDuration = 'interval 30 days');
CREATE TABLE IF NOT EXISTS bronze.raw_bars LIKE bronze.raw_events;
CREATE TABLE IF NOT EXISTS bronze.rejects (source STRING, raw STRING, error STRING);

CREATE TABLE IF NOT EXISTS ops.watermarks (source STRING, watermark_utc TIMESTAMP);
CREATE TABLE IF NOT EXISTS ops.run_log (job STRING, source STRING, run_id STRING, started_utc TIMESTAMP, duration_s DOUBLE,
  received BIGINT, kept BIGINT, rejected BIGINT, status STRING, message STRING, quota_remaining BIGINT);
CREATE TABLE IF NOT EXISTS ops.freshness (checked_utc TIMESTAMP, source STRING, last_published_utc TIMESTAMP,
  last_ingested_utc TIMESTAMP, lag_minutes DOUBLE);
CREATE TABLE IF NOT EXISTS ops.api_quota (checked_utc TIMESTAMP, source STRING, used BIGINT, quota BIGINT);
CREATE TABLE IF NOT EXISTS ops.incidents (opened_utc TIMESTAMP, source STRING, severity STRING, description STRING, resolved_utc TIMESTAMP);
