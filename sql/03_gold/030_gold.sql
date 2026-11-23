USE CATALOG IDENTIFIER(:catalog);

CREATE TABLE IF NOT EXISTS gold.model_predictions (
  hk_article STRING,
  hk_security STRING,
  predicted_magnitude INT,
  predicted_proba_high DOUBLE,
  model_name STRING,
  model_version STRING,
  scored_utc TIMESTAMP
);

CREATE OR REPLACE VIEW gold.dim_security AS
SELECT
  h.hk_security,
  h.bk_symbol AS symbol,
  r.name,
  r.sector,
  r.industry
FROM vault.hub_security h
JOIN (
  SELECT *
  FROM vault.sat_security_ref
  QUALIFY row_number() OVER (
    PARTITION BY hk_security
    ORDER BY load_ts DESC
  ) = 1
) r USING (hk_security);

CREATE OR REPLACE VIEW gold.dim_sector AS
SELECT
  sha2(upper(trim(sector)), 256) AS hk_sector,
  sector,
  count(*) AS nb_titres
FROM gold.dim_security
WHERE sector IS NOT NULL
GROUP BY sector;

CREATE OR REPLACE VIEW gold.dim_category AS
SELECT DISTINCT category
FROM business_vault.bv_article_enriched;

CREATE OR REPLACE VIEW gold.dim_date AS
SELECT
  d AS date,
  year(d) AS y,
  month(d) AS m,
  dayofweek(d) AS dow,
  date_format(d, 'yyyy-MM') AS ym
FROM (
  SELECT explode(
    sequence(
      DATE'2020-01-01',
      DATE'2030-12-31',
      INTERVAL 1 DAY
    )
  ) AS d
);

-- Cours intraday ajustés.
CREATE OR REPLACE VIEW gold.fact_bar_1m AS
SELECT
  b.hk_security,
  b.bar_ts,
  b.open * coalesce(f.adj_factor, 1.0) AS open,
  b.high * coalesce(f.adj_factor, 1.0) AS high,
  b.low * coalesce(f.adj_factor, 1.0) AS low,
  b.close * coalesce(f.adj_factor, 1.0) AS close,
  b.volume / coalesce(f.split_factor, 1.0) AS volume,
  coalesce(f.adj_factor, 1.0) AS adj_factor
FROM vault.nhsat_bar_1m b
LEFT JOIN business_vault.v_adjustment_factor f
  ON f.hk_security = b.hk_security
 AND f.d = to_date(
   from_utc_timestamp(b.bar_ts, 'America/New_York')
 );

-- CAR regroupés par article, titre et référence temporelle.
CREATE OR REPLACE VIEW gold.fact_news_impact AS

WITH car_pivot AS (
  SELECT
    hk_article,
    hk_security,
    coalesce(event_time_basis, 'published') AS event_time_basis,

    max(CASE
      WHEN window_end_min = 15 THEN car
    END) AS car_15,

    max(CASE
      WHEN window_end_min = 15 THEN t_stat
    END) AS t_stat_15,

    max(CASE
      WHEN window_end_min = 15 THEN p_value
    END) AS p_value_15,

    max(CASE
      WHEN window_end_min = 60 THEN car
    END) AS car,

    max(CASE
      WHEN window_end_min = 60 THEN t_stat
    END) AS t_stat,

    max(CASE
      WHEN window_end_min = 60 THEN p_value
    END) AS p_value,

    max(car_pre) AS car_pre,
    max(leak_flag) AS leak_flag

  FROM business_vault.bv_event_car
  WHERE model_version = (SELECT max(model_version) FROM business_vault.bv_event_car)
  GROUP BY
    hk_article,
    hk_security,
    coalesce(event_time_basis, 'published')
)

SELECT
  e.hk_article,
  l.hk_security,
  c.event_time_basis,

  e.published_utc,
  e.available_utc,
  e.session_type,
  e.session_date,
  e.reaction_start_utc,

  e.sentiment_score,
  e.sentiment_label,
  e.category,
  e.is_first,
  e.cluster_size,

  c.car_15,
  c.t_stat_15,
  c.p_value_15,

  c.car,
  c.t_stat,
  c.p_value,
  c.car_pre,
  c.leak_flag,
  (c.p_value < 0.05) AS significant,

  a.headline,
  left(a.summary, 500) AS summary,
  a.url,
  CASE WHEN lower(a.source_name) = 'benzinga' THEN 'Benzinga' ELSE a.source_name END AS source_name,
  a.bronze_ref,
  a.bronze_record_hash

FROM business_vault.bv_article_enriched e
JOIN vault.link_article_security l
  USING (hk_article)
JOIN vault.v_article_latest a
  USING (hk_article)
JOIN car_pivot c
  ON c.hk_article = e.hk_article
 AND c.hk_security = l.hk_security
WHERE e.is_first;

-- Rendements intraday consécutifs pour la volatilité pré-événement.
CREATE OR REPLACE VIEW gold.v_training_set AS

WITH bar_lag AS (
  SELECT
    hk_security,
    bar_ts,
    close,

    lag(bar_ts) OVER (
      PARTITION BY hk_security
      ORDER BY bar_ts
    ) AS prev_ts,

    lag(close) OVER (
      PARTITION BY hk_security
      ORDER BY bar_ts
    ) AS prev_close

  FROM gold.fact_bar_1m
  WHERE close > 0
),

bar_returns AS (
  SELECT
    hk_security,
    bar_ts,

    CASE
      WHEN prev_ts = bar_ts - INTERVAL 1 MINUTE
       AND prev_close > 0
      THEN ln(close / prev_close)
    END AS log_return

  FROM bar_lag
),

ticker_counts AS (
  SELECT
    hk_article,
    count(DISTINCT hk_security) AS n_tickers
  FROM vault.link_article_security
  GROUP BY hk_article
),

training_base AS (
  SELECT *
  FROM gold.fact_news_impact
  WHERE event_time_basis = 'published'
    AND car IS NOT NULL
),

pre_volatility AS (
  SELECT
    f.hk_article,
    f.hk_security,
    stddev_samp(r.log_return) AS pre_vol_15m,
    count(r.log_return) AS n_pre_returns

  FROM training_base f
  LEFT JOIN bar_returns r
    ON r.hk_security = f.hk_security
   AND r.bar_ts >= date_trunc('minute', f.published_utc)
                   - INTERVAL 15 MINUTES
   AND r.bar_ts < date_trunc('minute', f.published_utc)

  GROUP BY
    f.hk_article,
    f.hk_security
)

SELECT
  f.hk_article,
  f.hk_security,
  f.published_utc,
  f.source_name AS source,
  f.sentiment_label,
  f.sentiment_score,
  f.category,
  f.session_type,
  f.is_first,
  f.cluster_size,

  coalesce(tc.n_tickers, 1) AS n_tickers,

  v.pre_vol_15m,
  v.n_pre_returns,

  abs(f.car) AS abs_car

FROM training_base f

LEFT JOIN ticker_counts tc
  ON tc.hk_article = f.hk_article

LEFT JOIN pre_volatility v
  ON v.hk_article = f.hk_article
 AND v.hk_security = f.hk_security;

CREATE OR REPLACE VIEW gold.fact_realtime_feed AS
SELECT
  f.*,
  p.predicted_magnitude,
  p.predicted_proba_high
FROM gold.fact_news_impact f
LEFT JOIN gold.model_predictions p
  USING (hk_article, hk_security)
WHERE f.published_utc > (SELECT max(published_utc) FROM gold.fact_news_impact) - INTERVAL 2 DAYS;

CREATE TABLE IF NOT EXISTS gold.fact_model_metrics (
  run_id STRING,
  model_name STRING,
  model_version STRING,
  alias STRING,
  metric STRING,
  value DOUBLE,
  logged_utc TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.fact_drift (
  checked_utc TIMESTAMP,
  model_name STRING,
  feature STRING,
  psi DOUBLE
);

CREATE OR REPLACE VIEW gold.fact_sector_heatmap AS
SELECT
  s.sector,
  f.event_time_basis,
  date_trunc('day', f.published_utc) AS day,
  count(*) AS n_news,
  avg(f.sentiment_score) AS avg_sentiment,
  avg(f.car) AS avg_car
FROM gold.fact_news_impact f
JOIN gold.dim_security s USING (hk_security)
GROUP BY
  s.sector,
  f.event_time_basis,
  date_trunc('day', f.published_utc);

CREATE OR REPLACE VIEW gold.fact_platform_health AS
SELECT
  r.source,
  date_trunc('hour', r.started_utc) AS hour,
  sum(received) AS received,
  sum(kept) AS kept,
  sum(rejected) AS rejected,
  sum(CASE WHEN status <> 'SUCCESS' THEN 1 ELSE 0 END) AS failures,
  avg(duration_s) AS avg_duration_s
FROM ops.run_log r
GROUP BY 1, 2;

CREATE OR REPLACE VIEW gold.v_model_metrics_latest AS
SELECT * FROM gold.fact_model_metrics
WHERE logged_utc = (SELECT MAX(logged_utc) FROM gold.fact_model_metrics);
