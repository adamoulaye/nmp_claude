USE CATALOG IDENTIFIER(:catalog);

-- Silver : prix quotidiens Yahoo (non ajustés), typés, dédoublonnés, date de séance en heure de New York
CREATE TABLE IF NOT EXISTS silver.silver_prices_daily (
    symbol STRING,
    trade_date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume DOUBLE,
    dividends DOUBLE,
    splits DOUBLE,
    ingested_utc TIMESTAMP
)
CLUSTER BY (symbol, trade_date);

MERGE INTO silver.silver_prices_daily AS t
USING (
    WITH raw AS (
        SELECT
            b.symbol AS bronze_symbol,
            b.ingested_utc,
            from_json(
                b.payload,
                'Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE, `Adj Close` DOUBLE, Volume DOUBLE, Dividends DOUBLE, `Stock Splits` DOUBLE, _symbol STRING, _ts STRING'
            ) AS j
        FROM bronze.raw_bars b
        WHERE b.source = 'yahoo_daily'
    ),
    parsed AS (
        SELECT
            upper(trim(coalesce(nullif(trim(bronze_symbol), ''), j._symbol))) AS symbol,
            to_date(from_utc_timestamp(try_cast(j._ts AS TIMESTAMP), 'America/New_York')) AS trade_date,
            j.Open AS open,
            j.High AS high,
            j.Low AS low,
            j.Close AS close,
            j.`Adj Close` AS adj_close,
            j.Volume AS volume,
            coalesce(j.Dividends, 0.0) AS dividends,
            coalesce(j.`Stock Splits`, 0.0) AS splits,
            ingested_utc
        FROM raw
    ),
    valid AS (
        SELECT *
        FROM parsed
        WHERE symbol IS NOT NULL AND symbol <> ''
          AND trade_date IS NOT NULL
          AND open > 0 AND close > 0
          AND high >= low
          AND volume >= 0
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol, trade_date ORDER BY ingested_utc DESC) AS rn
        FROM valid
    )
    SELECT symbol, trade_date, open, high, low, close, adj_close, volume, dividends, splits, ingested_utc
    FROM ranked
    WHERE rn = 1
) AS s
ON t.symbol = s.symbol AND t.trade_date = s.trade_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Vault : titres (même clé de hachage que partout ailleurs)
MERGE INTO vault.hub_security AS t
USING (
    SELECT
        sha2(upper(trim(symbol)), 256) AS hk_security,
        upper(trim(symbol)) AS bk_symbol,
        min(ingested_utc) AS load_ts,
        'yahoo_daily' AS record_source
    FROM silver.silver_prices_daily
    GROUP BY symbol
) AS s
ON t.hk_security = s.hk_security
WHEN NOT MATCHED THEN INSERT *;

-- Vault : actions corporatives (splits : value = ratio ; dividendes : value = montant)
MERGE INTO vault.sat_corporate_action AS t
USING (
    SELECT hk_security, ex_date, action, value, load_ts, record_source
    FROM (
        SELECT
            sha2(upper(trim(symbol)), 256) AS hk_security,
            trade_date AS ex_date,
            'split' AS action,
            splits AS value,
            current_timestamp() AS load_ts,
            'yahoo_daily' AS record_source
        FROM silver.silver_prices_daily
        WHERE splits > 0
        UNION ALL
        SELECT
            sha2(upper(trim(symbol)), 256),
            trade_date,
            'dividend',
            dividends,
            current_timestamp(),
            'yahoo_daily'
        FROM silver.silver_prices_daily
        WHERE dividends > 0
    )
) AS s
ON t.hk_security = s.hk_security AND t.ex_date = s.ex_date AND t.action = s.action
WHEN NOT MATCHED THEN INSERT (hk_security, ex_date, action, value, load_ts, record_source)
VALUES (s.hk_security, s.ex_date, s.action, s.value, s.load_ts, s.record_source);
