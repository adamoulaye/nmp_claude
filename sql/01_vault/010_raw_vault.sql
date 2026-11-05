USE CATALOG IDENTIFIER(:catalog);

-- ===== HUBS (clés métier uniquement) =====
CREATE TABLE IF NOT EXISTS vault.hub_article (hk_article STRING NOT NULL, bk_article STRING NOT NULL, load_ts TIMESTAMP, record_source STRING);
CREATE TABLE IF NOT EXISTS vault.hub_security (hk_security STRING NOT NULL, bk_symbol STRING NOT NULL, load_ts TIMESTAMP, record_source STRING);

-- ===== LINKS =====
CREATE TABLE IF NOT EXISTS vault.link_article_security (hk_link STRING NOT NULL, hk_article STRING, hk_security STRING, load_ts TIMESTAMP, record_source STRING);

-- ===== SATELLITES (historisés par hashdiff : chaque mise à jour d'article = nouvelle ligne) =====
CREATE TABLE IF NOT EXISTS vault.sat_article_content (
  hk_article STRING, load_ts TIMESTAMP, hashdiff STRING, headline STRING, summary STRING, url STRING, source_name STRING,
  published_utc TIMESTAMP, updated_utc TIMESTAMP, first_seen_utc TIMESTAMP, ingested_utc TIMESTAMP,
  bronze_record_hash STRING, bronze_ref STRING, record_source STRING);          -- traçabilité vers Bronze
CREATE TABLE IF NOT EXISTS vault.sat_link_assoc (hk_link STRING, load_ts TIMESTAMP, confidence DOUBLE, method STRING);
CREATE TABLE IF NOT EXISTS vault.sat_security_ref (hk_security STRING, load_ts TIMESTAMP, hashdiff STRING, sector STRING, industry STRING, name STRING);
-- Données de mesure : lien/sat non historisé (insert-only), très gros volume
CREATE TABLE IF NOT EXISTS vault.nhsat_bar_1m (hk_security STRING, bar_ts TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
  volume DOUBLE, vwap DOUBLE, trade_count DOUBLE, load_ts TIMESTAMP, record_source STRING) CLUSTER BY (hk_security, bar_ts);
CREATE TABLE IF NOT EXISTS vault.sat_corporate_action (hk_security STRING, ex_date DATE, action STRING, value DOUBLE, load_ts TIMESTAMP, record_source STRING);

-- ===== CHARGEMENTS (idempotents) =====
MERGE INTO vault.hub_article t USING (
  SELECT sha2(upper(trim(concat('alpaca_news|', cast(article_id AS STRING)))), 256) hk_article,
         concat('alpaca_news|', cast(article_id AS STRING)) bk_article, min(ingested_utc) load_ts, 'alpaca_news' record_source
  FROM silver.silver_news GROUP BY article_id) s
ON t.hk_article = s.hk_article WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.sat_article_content t USING (
  SELECT sha2(upper(trim(concat('alpaca_news|', cast(article_id AS STRING)))), 256) hk_article, current_timestamp() load_ts,
         sha2(concat_ws('|', headline, summary, url, cast(updated_utc AS STRING)), 256) hashdiff,
         headline, summary, url, source_name, published_utc, updated_utc, first_seen_utc, ingested_utc,
         record_hash bronze_record_hash, bronze_ref, 'alpaca_news' record_source
  FROM silver.silver_news) s
ON t.hk_article = s.hk_article AND t.hashdiff = s.hashdiff WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.hub_security t USING (
  SELECT DISTINCT sha2(upper(trim(sym)), 256) hk_security, upper(trim(sym)) bk_symbol, current_timestamp() load_ts, 'alpaca_news' record_source
  FROM silver.silver_news LATERAL VIEW explode(symbols) e AS sym) s
ON t.hk_security = s.hk_security WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.link_article_security t USING (
  SELECT DISTINCT sha2(concat_ws('|', ha, hs), 256) hk_link, ha hk_article, hs hk_security, current_timestamp() load_ts, 'alpaca_news' record_source
  FROM (SELECT sha2(upper(trim(concat('alpaca_news|', cast(article_id AS STRING)))), 256) ha, sha2(upper(trim(sym)), 256) hs
        FROM silver.silver_news LATERAL VIEW explode(symbols) e AS sym)) s
ON t.hk_link = s.hk_link WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.nhsat_bar_1m t USING (
  SELECT sha2(upper(trim(symbol)), 256) hk_security, bar_ts, open, high, low, close, volume, vwap, trade_count, current_timestamp() load_ts, 'alpaca_bars' record_source
  FROM silver.silver_bars_1m) s
ON t.hk_security = s.hk_security AND t.bar_ts = s.bar_ts WHEN NOT MATCHED THEN INSERT *;

-- Vue : dernière version de chaque article (utilisée par le job d'enrichissement)
CREATE OR REPLACE VIEW vault.v_article_latest AS
SELECT * EXCEPT (rn) FROM (SELECT *, row_number() OVER (PARTITION BY hk_article ORDER BY updated_utc DESC, load_ts DESC) rn FROM vault.sat_article_content) WHERE rn = 1;
