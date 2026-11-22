USE CATALOG IDENTIFIER(:catalog);

CREATE OR REPLACE VIEW ops.v_alerts AS
WITH fr AS (SELECT * FROM ops.freshness WHERE checked_utc = (SELECT max(checked_utc) FROM ops.freshness)),
sess AS (SELECT (dayofweek(t) BETWEEN 2 AND 6 AND hour(t)*60 + minute(t) BETWEEN 570 AND 960) AS en_seance FROM (SELECT from_utc_timestamp(current_timestamp(),'America/New_York') AS t)),
last_run AS (SELECT source, max_by(status, started_utc) AS statut FROM ops.run_log WHERE job='ingest' AND started_utc >= current_timestamp() - INTERVAL 7 DAYS GROUP BY source),
bar AS (SELECT timestampdiff(MINUTE, max(bar_ts), current_timestamp()) AS age_min FROM gold.fact_bar_1m),
orph AS (SELECT COUNT(*) n FROM gold.fact_news_impact f WHERE NOT EXISTS (SELECT 1 FROM gold.dim_security d WHERE d.hk_security = f.hk_security)),
nopred AS (SELECT COUNT(*) n FROM gold.model_predictions p WHERE NOT EXISTS (SELECT 1 FROM gold.fact_news_impact f WHERE f.hk_article = p.hk_article AND f.hk_security = p.hk_security)),
dupsec AS (SELECT COUNT(*) - COUNT(DISTINCT hk_security) AS n FROM gold.dim_security)
SELECT a.*, current_timestamp() AS evalue_utc FROM (
SELECT 'source muette' AS alerte, fr.source AS detail, 'high' AS severite, (sess.en_seance AND fr.lag_minutes > 60) AS declenchee, round(fr.lag_minutes,1) AS valeur, '> 60 min en seance' AS seuil FROM fr CROSS JOIN sess
UNION ALL SELECT 'ingestion en echec', source, 'high', statut <> 'SUCCESS', CAST(NULL AS DOUBLE), 'dernier run <> SUCCESS' FROM last_run
UNION ALL SELECT 'desynchronisation texte-prix', 'fact_bar_1m', 'medium', (sess.en_seance AND bar.age_min > 30), CAST(bar.age_min AS DOUBLE), 'barres > 30 min en seance' FROM bar CROSS JOIN sess
UNION ALL SELECT 'controle qualite : orphelins Gold', 'fact_news_impact', 'high', n > 0, CAST(n AS DOUBLE), '> 0' FROM orph
UNION ALL SELECT 'controle qualite : predictions sans evenement', 'model_predictions', 'high', n > 0, CAST(n AS DOUBLE), '> 0' FROM nopred
UNION ALL SELECT 'controle qualite : cles dupliquees', 'dim_security', 'high', n > 0, CAST(n AS DOUBLE), '> 0' FROM dupsec
) a;
