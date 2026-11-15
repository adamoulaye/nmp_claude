USE CATALOG IDENTIFIER(:catalog);

CREATE OR REPLACE VIEW gold.fact_live_news AS
WITH base AS (
  SELECT e.hk_article, l.hk_security, e.published_utc, e.available_utc, e.session_type,
         e.sentiment_score, e.sentiment_label, e.category, e.cluster_size,
         a.headline, a.summary, a.url, a.source_name
  FROM business_vault.bv_article_enriched e
  JOIN vault.link_article_security l USING (hk_article)
  JOIN vault.v_article_latest a USING (hk_article)
  WHERE e.is_first AND a.headline NOT LIKE 'TEST-%'
),
car60 AS (
  SELECT hk_article, hk_security,
         max(CASE WHEN window_end_min = 60 THEN car END) AS car,
         max(CASE WHEN window_end_min = 60 THEN p_value END) AS p_value
  FROM business_vault.bv_event_car
  WHERE model_version = (SELECT max(model_version) FROM business_vault.bv_event_car)
  GROUP BY hk_article, hk_security
)
SELECT b.*, s.symbol, s.name AS security_name, s.sector,
       c.car, c.p_value,
       CASE
         WHEN c.car IS NOT NULL THEN 'calculé'
         WHEN b.session_type = 'pendant'
              AND dayofweek(to_date(from_utc_timestamp(b.published_utc, 'America/New_York'))) BETWEEN 2 AND 6
              AND hour(from_utc_timestamp(b.published_utc, 'America/New_York')) * 60
                  + minute(from_utc_timestamp(b.published_utc, 'America/New_York')) BETWEEN 570 AND 900
           THEN 'en attente'
         ELSE 'hors séance'
       END AS impact_status
FROM base b
LEFT JOIN gold.dim_security s USING (hk_security)
LEFT JOIN car60 c USING (hk_article, hk_security)
WHERE b.published_utc >= (SELECT max(published_utc) FROM base) - INTERVAL 2 DAYS
