USE CATALOG IDENTIFIER(:catalog);

CREATE TABLE IF NOT EXISTS business_vault.bv_article_enriched (
  hk_article STRING,
  published_utc TIMESTAMP,
  updated_utc TIMESTAMP,
  first_seen_utc TIMESTAMP,
  available_utc TIMESTAMP,
  cluster_id STRING,
  is_first BOOLEAN,
  dup_rank INT,
  cluster_size INT,
  session_type STRING,
  session_date DATE,
  reaction_start_utc TIMESTAMP,
  positive DOUBLE,
  negative DOUBLE,
  neutral DOUBLE,
  sentiment_score DOUBLE,
  sentiment_label STRING,
  category STRING,
  model_version STRING,
  load_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS business_vault.bv_event_car (
  hk_article STRING,
  hk_security STRING,
  event_time_basis STRING,
  window_start_min INT,
  window_end_min INT,
  car DOUBLE,
  t_stat DOUBLE,
  p_value DOUBLE,
  alpha DOUBLE,
  beta DOUBLE,
  n_est INT,
  car_pre DOUBLE,
  leak_flag BOOLEAN,
  model_version STRING,
  load_ts TIMESTAMP
);


CREATE TABLE IF NOT EXISTS business_vault.bv_anomaly (
  hk_security STRING,
  bar_ts TIMESTAMP,
  anomaly_score DOUBLE,
  anomaly_type STRING,
  model_version STRING
);

-- Facteurs d'ajustement quotidiens.
CREATE OR REPLACE VIEW business_vault.v_adjustment_factor AS

WITH daily AS (
  SELECT
    hk_security,
    to_date(from_utc_timestamp(bar_ts, 'America/New_York')) AS d,
    max_by(close, bar_ts) AS c
  FROM vault.nhsat_bar_1m
  GROUP BY 1, 2
),

daily_prev AS (
  SELECT
    hk_security,
    d,
    lag(c) OVER (
      PARTITION BY hk_security
      ORDER BY d
    ) AS prev_c
  FROM daily
),

ev AS (
  SELECT
    a.hk_security,
    a.ex_date AS d,

    exp(sum(ln(
      CASE
        WHEN a.action = 'split' AND a.value > 0
          THEN 1.0 / a.value
        WHEN a.action = 'dividend' AND p.prev_c > 0
          THEN 1.0 - a.value / p.prev_c
        ELSE 1.0
      END
    ))) AS f,

    exp(sum(ln(
      CASE
        WHEN a.action = 'split' AND a.value > 0
          THEN 1.0 / a.value
        ELSE 1.0
      END
    ))) AS f_split

  FROM vault.sat_corporate_action a
  LEFT JOIN daily_prev p
    ON p.hk_security = a.hk_security
   AND p.d = a.ex_date
  GROUP BY a.hk_security, a.ex_date
),

x AS (
  SELECT
    dp.hk_security,
    dp.d,
    coalesce(ev.f, 1.0) AS f,
    coalesce(ev.f_split, 1.0) AS f_split
  FROM daily_prev dp
  LEFT JOIN ev
    ON ev.hk_security = dp.hk_security
   AND ev.d = dp.d
)

SELECT
  hk_security,
  d,
  coalesce(
    exp(sum(ln(f)) OVER w),
    1.0
  ) AS adj_factor,
  coalesce(
    exp(sum(ln(f_split)) OVER w),
    1.0
  ) AS split_factor
FROM x
WINDOW w AS (
  PARTITION BY hk_security
  ORDER BY d DESC
  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
);

-- Vue point-in-time.
-- Table PIT
CREATE OR REPLACE VIEW business_vault.pit_article AS
SELECT
  e.hk_article,
  e.available_utc,
  c.headline,
  c.url,
  e.sentiment_score,
  e.category,
  e.cluster_id,
  e.is_first
FROM business_vault.bv_article_enriched e
JOIN vault.v_article_latest c USING (hk_article);