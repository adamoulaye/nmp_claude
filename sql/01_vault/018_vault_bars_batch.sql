USE CATALOG IDENTIFIER(:catalog);

-- ============================================================
-- 1. CREATION DES TITRES MANQUANTS DANS LE HUB
-- ============================================================

MERGE INTO vault.hub_security AS t

USING (
    SELECT DISTINCT
        sha2(upper(trim(symbol)), 256) AS hk_security,
        upper(trim(symbol)) AS bk_symbol,
        current_timestamp() AS load_ts,
        'alpaca_bars_batch' AS record_source

    FROM silver.silver_bars_batch

    WHERE symbol IS NOT NULL
      AND trim(symbol) <> ''

) AS s

ON t.hk_security = s.hk_security

WHEN NOT MATCHED THEN INSERT (
    hk_security,
    bk_symbol,
    load_ts,
    record_source
)
VALUES (
    s.hk_security,
    s.bk_symbol,
    s.load_ts,
    s.record_source
);


-- ============================================================
-- 2. INSERTION DES BARRES DANS LE VAULT
-- ============================================================

MERGE INTO vault.nhsat_bar_1m AS t

USING (
    SELECT
        sha2(upper(trim(symbol)), 256) AS hk_security,
        bar_ts,
        open,
        high,
        low,
        close,
        volume,
        vwap,
        trade_count,
        current_timestamp() AS load_ts,
        'alpaca_bars_batch' AS record_source

    FROM silver.silver_bars_batch

    WHERE symbol IS NOT NULL
      AND trim(symbol) <> ''
      AND bar_ts IS NOT NULL

) AS s

ON t.hk_security = s.hk_security
AND t.bar_ts = s.bar_ts

WHEN NOT MATCHED THEN INSERT (
    hk_security,
    bar_ts,
    open,
    high,
    low,
    close,
    volume,
    vwap,
    trade_count,
    load_ts,
    record_source
)
VALUES (
    s.hk_security,
    s.bar_ts,
    s.open,
    s.high,
    s.low,
    s.close,
    s.volume,
    s.vwap,
    s.trade_count,
    s.load_ts,
    s.record_source
);
