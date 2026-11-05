USE CATALOG IDENTIFIER(:catalog);

CREATE TABLE IF NOT EXISTS gold.model_predictions (hk_article STRING, hk_security STRING, predicted_magnitude INT, predicted_proba_high DOUBLE, model_name STRING, model_version STRING, scored_utc TIMESTAMP);

CREATE OR REPLACE VIEW gold.dim_security AS
SELECT h.hk_security, h.bk_symbol symbol, r.name, r.sector, r.industry FROM vault.hub_security h
LEFT JOIN (SELECT * FROM vault.sat_security_ref QUALIFY row_number() OVER (PARTITION BY hk_security ORDER BY load_ts DESC)=1) r USING (hk_security);

CREATE OR REPLACE VIEW gold.dim_category AS SELECT DISTINCT category FROM business_vault.bv_article_enriched;

CREATE OR REPLACE VIEW gold.dim_date AS
SELECT d date, year(d) y, month(d) m, dayofweek(d) dow, date_format(d,'yyyy-MM') ym FROM (SELECT explode(sequence(DATE'2020-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) d);

-- Bougies 1 min ajustées (jamais d'écrasement du brut)
CREATE OR REPLACE VIEW gold.fact_bar_1m AS
SELECT b.hk_security, b.bar_ts, b.open*coalesce(f.adj_factor, 1.0) open, b.high*coalesce(f.adj_factor, 1.0) high, b.low*coalesce(f.adj_factor, 1.0) low, b.close*coalesce(f.adj_factor, 1.0) close,
       b.volume/coalesce(f.split_factor, 1.0) volume, coalesce(f.adj_factor, 1.0) adj_factor
FROM vault.nhsat_bar_1m b LEFT JOIN business_vault.v_adjustment_factor f
  ON f.hk_security=b.hk_security AND f.d=to_date(from_utc_timestamp(b.bar_ts,'America/New_York'));

-- Fait central : chaque chiffre remonte à l'article (hk_article, url) puis à Bronze (bronze_ref, record_hash)
CREATE OR REPLACE VIEW gold.fact_news_impact AS
SELECT e.hk_article, l.hk_security, e.published_utc, e.available_utc, e.session_type, e.session_date, e.reaction_start_utc,
       e.sentiment_score, e.sentiment_label, e.category, e.is_first, e.cluster_size,
       c.car, c.t_stat, c.p_value, c.car_pre, c.leak_flag, (c.p_value < 0.05) AS significant,
       a.headline, a.summary, a.url, a.source_name, a.bronze_ref, a.bronze_record_hash
FROM business_vault.bv_article_enriched e
JOIN vault.link_article_security l USING (hk_article)
JOIN vault.v_article_latest a USING (hk_article)
LEFT JOIN business_vault.bv_event_car c ON c.hk_article=e.hk_article AND c.hk_security=l.hk_security AND c.window_end_min=60
WHERE e.is_first;   -- signal uniquement ; les reprises restent dans bv_article_enriched

CREATE OR REPLACE VIEW gold.v_training_set AS
SELECT f.hk_article, f.hk_security, f.published_utc, f.source_name AS source, f.sentiment_label, f.sentiment_score, f.category, f.session_type,
       f.is_first, f.cluster_size, 1 AS n_tickers, 0.0 AS pre_vol_15m, abs(f.car) AS abs_car
FROM gold.fact_news_impact f WHERE f.car IS NOT NULL;

CREATE OR REPLACE VIEW gold.fact_realtime_feed AS
SELECT f.*, p.predicted_magnitude, p.predicted_proba_high FROM gold.fact_news_impact f
LEFT JOIN gold.model_predictions p USING (hk_article, hk_security) WHERE f.published_utc > current_timestamp() - INTERVAL 2 DAYS;

CREATE TABLE IF NOT EXISTS gold.fact_model_metrics (run_id STRING, model_name STRING, model_version STRING, alias STRING, metric STRING, value DOUBLE, logged_utc TIMESTAMP);
CREATE TABLE IF NOT EXISTS gold.fact_drift (checked_utc TIMESTAMP, model_name STRING, feature STRING, psi DOUBLE);

CREATE OR REPLACE VIEW gold.fact_sector_heatmap AS
SELECT s.sector, date_trunc('day', f.published_utc) day, count(*) n_news, avg(f.sentiment_score) avg_sentiment, avg(f.car) avg_car
FROM gold.fact_news_impact f JOIN gold.dim_security s USING (hk_security) GROUP BY 1,2;

CREATE OR REPLACE VIEW gold.fact_platform_health AS
SELECT r.source, date_trunc('hour', r.started_utc) hour, sum(received) received, sum(kept) kept, sum(rejected) rejected,
       sum(CASE WHEN status<>'SUCCESS' THEN 1 ELSE 0 END) failures, avg(duration_s) avg_duration_s
FROM ops.run_log r GROUP BY 1,2;
