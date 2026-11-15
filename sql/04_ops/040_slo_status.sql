USE CATALOG IDENTIFIER(:catalog);

CREATE OR REPLACE VIEW ops.v_slo_status AS
WITH ing AS (
  SELECT source, COUNT(*) n, SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) ok, SUM(received) rec, SUM(rejected) rej
  FROM ops.run_log WHERE job='ingest' AND started_utc >= current_timestamp() - INTERVAL 7 DAYS GROUP BY source),
fr AS (SELECT * FROM ops.freshness WHERE checked_utc = (SELECT max(checked_utc) FROM ops.freshness)),
dur AS (SELECT SUM(d) AS d FROM (SELECT job, max_by(duration_s, started_utc) AS d FROM ops.run_log WHERE job IN ('enrich','car') AND status='SUCCESS' AND kept > 0 GROUP BY job)),
pub AS (SELECT COUNT(*) n, SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) ok FROM ops.run_log WHERE job='publish' AND started_utc >= current_timestamp() - INTERVAL 7 DAYS),
bars AS (SELECT date(bar_ts) d, COUNT(DISTINCT hk_security) n FROM gold.fact_bar_1m WHERE bar_ts >= current_date() - INTERVAL 7 DAYS GROUP BY 1)
SELECT s.*, current_timestamp() AS evalue_utc FROM (
SELECT 'S1 disponibilite ingestion 7j (%)' AS slo, source AS detail, '>= 95' AS cible, round(100.0*ok/n,1) AS valeur, CASE WHEN ok/n >= 0.95 THEN 'OK' ELSE 'KO' END AS statut FROM ing
UNION ALL SELECT 'S2 depuis derniere ingestion (min)', source, '< 60 en seance', round(lag_minutes,1),
  CASE WHEN dayofweek(from_utc_timestamp(checked_utc,'America/New_York')) BETWEEN 2 AND 6
        AND hour(from_utc_timestamp(checked_utc,'America/New_York'))*60 + minute(from_utc_timestamp(checked_utc,'America/New_York')) BETWEEN 570 AND 960
       THEN CASE WHEN lag_minutes < 60 THEN 'OK' ELSE 'KO' END ELSE 'HORS_SEANCE' END FROM fr
UNION ALL SELECT 'S3 rejets 7j (%)', source, '< 5', round(100.0*rej/nullif(rec,0),2), CASE WHEN rec = 0 THEN 'N/A' WHEN rej/rec < 0.05 THEN 'OK' ELSE 'KO' END FROM ing
UNION ALL SELECT 'S4 enrich+car dernier run incremental (s)', 'batch', '< 3600', round(d,0), CASE WHEN d < 3600 THEN 'OK' ELSE 'KO' END FROM dur
UNION ALL SELECT 'S5 titres avec barres par jour (min)', 'fact_bar_1m', '>= 28', cast(min(n) AS DOUBLE), CASE WHEN min(n) >= 28 THEN 'OK' ELSE 'KO' END FROM bars
UNION ALL SELECT 'S6 publication 7j (%)', 'publish', '>= 95', round(100.0*ok/n,1), CASE WHEN ok/n >= 0.95 THEN 'OK' ELSE 'KO' END FROM pub
UNION ALL SELECT 'S7 quota API (%)', 'ops.api_quota', '< 80', CAST(NULL AS DOUBLE), 'NON_INSTRUMENTE'
) s;
