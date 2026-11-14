USE CATALOG IDENTIFIER(:catalog);

CREATE TABLE IF NOT EXISTS silver.silver_filings_8k (
    filing_id STRING,
    cik STRING,
    symbol STRING,
    accession_number STRING,
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
    record_hash STRING,
    bronze_ref STRING
);

MERGE INTO silver.silver_filings_8k AS t
USING (
    WITH parsed AS (
        SELECT
            record_id AS filing_id,
            get_json_object(payload, '$._cik') AS cik,
            upper(trim(coalesce(get_json_object(payload, '$._symbol'), symbol))) AS symbol,
            get_json_object(payload, '$.accessionNumber') AS accession_number,
            get_json_object(payload, '$.form') AS form,
            to_date(get_json_object(payload, '$.filingDate')) AS filing_date,
            to_date(get_json_object(payload, '$.reportDate')) AS report_date,
            to_timestamp(get_json_object(payload, '$.acceptanceDateTime')) AS acceptance_utc,
            get_json_object(payload, '$.items') AS items,
            get_json_object(payload, '$.isXBRL') = '1' AS is_xbrl,
            get_json_object(payload, '$.primaryDocument') AS primary_document,
            ingested_utc,
            record_hash,
            concat(source, ':', record_id, ':', record_hash) AS bronze_ref
        FROM bronze.raw_events
        WHERE source = 'sec_8k'
    ),
    valid AS (
        SELECT * FROM parsed
        WHERE filing_id IS NOT NULL AND cik IS NOT NULL AND symbol <> ''
          AND accession_number IS NOT NULL AND form = '8-K' AND acceptance_utc IS NOT NULL
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY filing_id ORDER BY ingested_utc DESC) AS rn
        FROM valid
    )
    SELECT
        filing_id, cik, symbol, accession_number, form, filing_date, report_date,
        acceptance_utc, items,
        CASE WHEN items IS NULL OR trim(items) = '' THEN array()
             ELSE transform(split(items, ','), x -> trim(x)) END AS items_array,
        is_xbrl, primary_document,
        concat('https://www.sec.gov/Archives/edgar/data/', regexp_replace(cik, '^0+', ''), '/',
               replace(accession_number, '-', ''), '/', primary_document) AS document_url,
        ingested_utc, record_hash, bronze_ref
    FROM ranked
    WHERE rn = 1
) AS s
ON t.filing_id = s.filing_id
WHEN MATCHED AND s.record_hash <> t.record_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
