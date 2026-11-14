USE CATALOG IDENTIFIER(:catalog);

CREATE TABLE IF NOT EXISTS vault.hub_filing (
    hk_filing STRING,
    bk_filing STRING,
    load_ts TIMESTAMP,
    record_source STRING
);

CREATE TABLE IF NOT EXISTS vault.link_filing_security (
    hk_link STRING,
    hk_filing STRING,
    hk_security STRING,
    load_ts TIMESTAMP,
    record_source STRING
);

CREATE TABLE IF NOT EXISTS vault.sat_filing_content (
    hk_filing STRING,
    load_ts TIMESTAMP,
    hashdiff STRING,
    form STRING,
    filing_date DATE,
    report_date DATE,
    acceptance_utc TIMESTAMP,
    items STRING,
    items_array ARRAY<STRING>,
    is_xbrl BOOLEAN,
    primary_document STRING,
    document_url STRING,
    ingested_utc TIMESTAMP,
    bronze_record_hash STRING,
    bronze_ref STRING,
    record_source STRING
);

MERGE INTO vault.hub_filing t
USING (
    SELECT
        sha2(upper(trim(concat('sec_8k', '|', filing_id))), 256) AS hk_filing,
        concat('sec_8k', '|', filing_id) AS bk_filing,
        min(ingested_utc) AS load_ts,
        'sec_8k' AS record_source
    FROM silver.silver_filings_8k
    GROUP BY filing_id
) s
ON t.hk_filing = s.hk_filing
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.sat_filing_content t
USING (
    SELECT
        sha2(upper(trim(concat('sec_8k', '|', filing_id))), 256) AS hk_filing,
        current_timestamp() AS load_ts,
        sha2(concat_ws('|', form, cast(filing_date AS STRING), cast(report_date AS STRING), items, primary_document), 256) AS hashdiff,
        form, filing_date, report_date, acceptance_utc, items, items_array,
        is_xbrl, primary_document, document_url, ingested_utc,
        record_hash AS bronze_record_hash, bronze_ref,
        'sec_8k' AS record_source
    FROM silver.silver_filings_8k
) s
ON t.hk_filing = s.hk_filing AND t.hashdiff = s.hashdiff
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO vault.link_filing_security t
USING (
    SELECT DISTINCT
        sha2(concat_ws('|', hk_filing, hk_security), 256) AS hk_link,
        hk_filing,
        hk_security,
        current_timestamp() AS load_ts,
        'sec_8k' AS record_source
    FROM (
        SELECT
            sha2(upper(trim(concat('sec_8k', '|', filing_id))), 256) AS hk_filing,
            sha2(upper(trim(symbol)), 256) AS hk_security
        FROM silver.silver_filings_8k
        WHERE symbol IS NOT NULL AND trim(symbol) <> ''
    ) x
) s
ON t.hk_link = s.hk_link
WHEN NOT MATCHED THEN INSERT *;
