USE CATALOG IDENTIFIER(:catalog);

CREATE TABLE IF NOT EXISTS silver.silver_bars_batch (
    symbol STRING,
    bar_ts TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    vwap DOUBLE,
    trade_count DOUBLE,
    ingested_utc TIMESTAMP
)
CLUSTER BY (symbol, bar_ts);

MERGE INTO silver.silver_bars_batch AS t
USING (
    WITH parsed AS (
        SELECT
            COALESCE(
                NULLIF(upper(trim(b.symbol)), ''),
                upper(trim(get_json_object(b.payload, '$._symbol')))
            ) AS symbol,

            TRY_CAST(
                get_json_object(b.payload, '$.t') AS TIMESTAMP
            ) AS bar_ts,

            TRY_CAST(get_json_object(b.payload, '$.o') AS DOUBLE) AS open,
            TRY_CAST(get_json_object(b.payload, '$.h') AS DOUBLE) AS high,
            TRY_CAST(get_json_object(b.payload, '$.l') AS DOUBLE) AS low,
            TRY_CAST(get_json_object(b.payload, '$.c') AS DOUBLE) AS close,
            TRY_CAST(get_json_object(b.payload, '$.v') AS DOUBLE) AS volume,
            TRY_CAST(get_json_object(b.payload, '$.vw') AS DOUBLE) AS vwap,
            TRY_CAST(get_json_object(b.payload, '$.n') AS DOUBLE) AS trade_count,
            b.ingested_utc

        FROM bronze.raw_bars b
        WHERE b.source = 'alpaca_bars_1m'
    ),

    valid AS (
        SELECT *
        FROM parsed
        WHERE symbol IS NOT NULL
          AND symbol <> ''
          AND bar_ts IS NOT NULL
          AND open > 0
          AND close > 0
          AND high >= low
          AND high >= open
          AND high >= close
          AND low <= open
          AND low <= close
          AND volume >= 0
    ),

    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, bar_ts
                ORDER BY ingested_utc DESC
            ) AS rn
        FROM valid
    )

    SELECT
        symbol,
        bar_ts,
        open,
        high,
        low,
        close,
        volume,
        vwap,
        trade_count,
        ingested_utc
    FROM ranked
    WHERE rn = 1

) AS s

ON t.symbol = s.symbol
AND t.bar_ts = s.bar_ts

WHEN MATCHED AND s.ingested_utc > t.ingested_utc THEN
    UPDATE SET
        t.open = s.open,
        t.high = s.high,
        t.low = s.low,
        t.close = s.close,
        t.volume = s.volume,
        t.vwap = s.vwap,
        t.trade_count = s.trade_count,
        t.ingested_utc = s.ingested_utc

WHEN NOT MATCHED THEN INSERT (
    symbol,
    bar_ts,
    open,
    high,
    low,
    close,
    volume,
    vwap,
    trade_count,
    ingested_utc
)
VALUES (
    s.symbol,
    s.bar_ts,
    s.open,
    s.high,
    s.low,
    s.close,
    s.volume,
    s.vwap,
    s.trade_count,
    s.ingested_utc
);
