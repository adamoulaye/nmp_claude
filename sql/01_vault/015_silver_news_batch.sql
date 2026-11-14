USE CATALOG IDENTIFIER(:catalog);

CREATE TABLE IF NOT EXISTS silver.silver_news_batch (
    source STRING,
    article_id STRING,
    headline STRING,
    summary STRING,
    source_name STRING,
    url STRING,
    symbols ARRAY<STRING>,
    published_utc TIMESTAMP,
    updated_utc TIMESTAMP,
    first_seen_utc TIMESTAMP,
    ingested_utc TIMESTAMP,
    record_hash STRING,
    bronze_ref STRING
);

MERGE INTO silver.silver_news_batch AS t
USING (
    WITH parsed AS (
        SELECT
            source,
            record_id,
            symbol,
            published_utc,
            updated_utc,
            ingested_utc,
            record_hash,
            payload,
            get_json_object(payload, '$.id') AS article_id,
            get_json_object(payload, '$.headline') AS headline,
            get_json_object(payload, '$.summary') AS raw_summary,
            get_json_object(payload, '$.source') AS source_name,
            get_json_object(payload, '$.url') AS url,
            get_json_object(payload, '$.related') AS related,
            get_json_object(payload, '$._symbol') AS payload_symbol,
            get_json_object(payload, '$.datetime') AS unix_datetime,
            from_json(
                get_json_object(payload, '$.symbols'),
                'ARRAY<STRING>'
            ) AS alpaca_symbols
        FROM bronze.raw_events
        WHERE source IN ('finnhub_news', 'alpaca_news')
    ),
    normalized AS (
        SELECT
            source,
            article_id,
            headline,
            CASE
                WHEN raw_summary RLIKE '^https?://' THEN ''
                ELSE raw_summary
            END AS summary,
            source_name,
            url,
            CASE
                WHEN source = 'alpaca_news'
                    THEN COALESCE(alpaca_symbols, array())
                ELSE array_distinct(
                    filter(
                        transform(
                            split(
                                concat_ws(
                                    ',',
                                    COALESCE(symbol, ''),
                                    COALESCE(payload_symbol, ''),
                                    COALESCE(related, '')
                                ),
                                ','
                            ),
                            x -> upper(trim(x))
                        ),
                        x -> x <> ''
                    )
                )
            END AS symbols,
            COALESCE(
                published_utc,
                TRY_CAST(
                    from_unixtime(TRY_CAST(unix_datetime AS BIGINT))
                    AS TIMESTAMP
                )
            ) AS published_utc,
            updated_utc,
            ingested_utc AS first_seen_utc,
            ingested_utc,
            record_hash,
            concat(source, ':', record_id, ':', record_hash) AS bronze_ref
        FROM parsed
        WHERE article_id IS NOT NULL
          AND headline IS NOT NULL
    )
    SELECT * FROM normalized
) AS s
ON t.article_id = s.article_id
AND t.bronze_ref = s.bronze_ref
WHEN NOT MATCHED THEN INSERT *;
