USE CATALOG IDENTIFIER(:catalog);

-- Chemin flux : silver.silver_news / silver.silver_bars_1m -> Vault (idempotent, dédoublonné)

MERGE INTO vault.hub_article t
USING (
    SELECT sha2(upper(trim(concat('alpaca_news|', cast(article_id AS STRING)))), 256) AS hk_article,
           concat('alpaca_news|', cast(article_id AS STRING)) AS bk_article,
           min(ingested_utc) AS load_ts,
           'alpaca_news' AS record_source
    FROM silver.silver_news
    WHERE article_id IS NOT NULL
    GROUP BY article_id
) s
ON t.hk_article = s.hk_article
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.sat_article_content t
USING (
    SELECT sha2(upper(trim(concat('alpaca_news|', cast(article_id AS STRING)))), 256) AS hk_article,
           current_timestamp() AS load_ts,
           sha2(concat_ws('|', headline, summary, url, cast(updated_utc AS STRING)), 256) AS hashdiff,
           headline, summary, url,
           min(source_name) AS source_name,
           min(published_utc) AS published_utc,
           updated_utc,
           min(first_seen_utc) AS first_seen_utc,
           min(ingested_utc) AS ingested_utc,
           concat_ws(',', sort_array(collect_set(record_hash))) AS bronze_record_hash,
           concat_ws(',', sort_array(collect_set(bronze_ref))) AS bronze_ref,
           'alpaca_news' AS record_source
    FROM silver.silver_news
    WHERE article_id IS NOT NULL
    GROUP BY article_id, headline, summary, url, updated_utc
) s
ON t.hk_article = s.hk_article AND t.hashdiff = s.hashdiff
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.hub_security t
USING (
    SELECT DISTINCT sha2(upper(trim(sym)), 256) AS hk_security, upper(trim(sym)) AS bk_symbol,
           current_timestamp() AS load_ts, 'news_stream' AS record_source
    FROM silver.silver_news LATERAL VIEW explode(symbols) e AS sym
    WHERE sym IS NOT NULL AND trim(sym) <> ''
) s
ON t.hk_security = s.hk_security
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.link_article_security t
USING (
    SELECT DISTINCT sha2(concat_ws('|', ha, hs), 256) AS hk_link, ha AS hk_article, hs AS hk_security,
           current_timestamp() AS load_ts, 'alpaca_news' AS record_source
    FROM (SELECT sha2(upper(trim(concat('alpaca_news|', cast(article_id AS STRING)))), 256) AS ha,
                 sha2(upper(trim(sym)), 256) AS hs
          FROM silver.silver_news LATERAL VIEW explode(symbols) e AS sym
          WHERE article_id IS NOT NULL AND sym IS NOT NULL AND trim(sym) <> '')
) s
ON t.hk_link = s.hk_link
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.nhsat_bar_1m t
USING (
    SELECT sha2(upper(trim(symbol)), 256) AS hk_security, bar_ts, open, high, low, close, volume, vwap, trade_count,
           current_timestamp() AS load_ts, 'alpaca_bars_stream' AS record_source
    FROM silver.silver_bars_1m
    WHERE symbol IS NOT NULL AND bar_ts IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY symbol, bar_ts ORDER BY ingested_utc DESC) = 1
) s
ON t.hk_security = s.hk_security AND t.bar_ts = s.bar_ts
WHEN NOT MATCHED THEN INSERT *;
