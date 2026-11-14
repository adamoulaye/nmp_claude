USE CATALOG IDENTIFIER(:catalog);

-- 1. HUB ARTICLES
MERGE INTO vault.hub_article t
USING (
    SELECT
        sha2(upper(trim(concat(source, '|', cast(article_id AS STRING)))), 256) AS hk_article,
        concat(source, '|', cast(article_id AS STRING)) AS bk_article,
        min(ingested_utc) AS load_ts,
        source AS record_source
    FROM silver.silver_news_batch
    GROUP BY source, article_id
) s
ON t.hk_article = s.hk_article
WHEN NOT MATCHED THEN INSERT *;

-- 2. SATELLITE CONTENU
MERGE INTO vault.sat_article_content t
USING (
    SELECT
        sha2(upper(trim(concat(source, '|', cast(article_id AS STRING)))), 256) AS hk_article,
        current_timestamp() AS load_ts,
        sha2(concat_ws('|', headline, summary, url, cast(updated_utc AS STRING)), 256) AS hashdiff,
        headline,
        summary,
        url,
        min(source_name) AS source_name,
        min(published_utc) AS published_utc,
        updated_utc,
        min(first_seen_utc) AS first_seen_utc,
        min(ingested_utc) AS ingested_utc,
        concat_ws(',', sort_array(collect_set(record_hash))) AS bronze_record_hash,
        concat_ws(',', sort_array(collect_set(bronze_ref))) AS bronze_ref,
        source AS record_source
    FROM silver.silver_news_batch
    GROUP BY
        source, article_id, headline, summary, url, updated_utc
) s
ON t.hk_article = s.hk_article AND t.hashdiff = s.hashdiff
WHEN NOT MATCHED THEN INSERT *;

-- 3. HUB TITRES
MERGE INTO vault.hub_security t
USING (
    SELECT DISTINCT
        sha2(upper(trim(sym)), 256) AS hk_security,
        upper(trim(sym)) AS bk_symbol,
        current_timestamp() AS load_ts,
        'news_batch' AS record_source
    FROM silver.silver_news_batch
    LATERAL VIEW explode(symbols) e AS sym
    WHERE sym IS NOT NULL AND trim(sym) <> ''
) s
ON t.hk_security = s.hk_security
WHEN NOT MATCHED THEN INSERT *;

-- 4. LIENS ARTICLES - TITRES
MERGE INTO vault.link_article_security t
USING (
    SELECT DISTINCT
        sha2(concat_ws('|', hk_article, hk_security), 256) AS hk_link,
        hk_article,
        hk_security,
        current_timestamp() AS load_ts,
        'news_batch' AS record_source
    FROM (
        SELECT
            sha2(upper(trim(concat(source, '|', cast(article_id AS STRING)))), 256) AS hk_article,
            sha2(upper(trim(sym)), 256) AS hk_security
        FROM silver.silver_news_batch
        LATERAL VIEW explode(symbols) e AS sym
        WHERE sym IS NOT NULL AND trim(sym) <> ''
    ) x
) s
ON t.hk_link = s.hk_link
WHEN NOT MATCHED THEN INSERT *;
