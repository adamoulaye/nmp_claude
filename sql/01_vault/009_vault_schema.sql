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
-- Vue : dernière version de chaque article (utilisée par le job d'enrichissement)
CREATE OR REPLACE VIEW vault.v_article_latest AS
SELECT * EXCEPT (rn) FROM (SELECT *, row_number() OVER (PARTITION BY hk_article ORDER BY updated_utc DESC, load_ts DESC) rn FROM vault.sat_article_content) WHERE rn = 1;
