USE CATALOG IDENTIFIER(:catalog);

CREATE OR REPLACE VIEW gold.dim_source AS
SELECT * FROM VALUES
  ('finnhub_news', 'Finnhub', 'Actualités', 'API REST company-news', 'Job Databricks (watermark)', 'bronze.raw_events'),
  ('alpaca_news', 'Alpaca News', 'Actualités', 'API REST /v1beta1/news', 'Job Databricks (watermark)', 'bronze.raw_events'),
  ('sec_8k', 'SEC EDGAR 8-K', 'Dépôts réglementaires', 'API submissions data.sec.gov', 'Job Databricks (watermark)', 'bronze.raw_events'),
  ('alpaca_bars_1m', 'Alpaca Market Data (IEX)', 'Marché', 'API REST barres 1 minute IEX', 'Export local puis job Databricks', 'bronze.raw_bars'),
  ('alpaca_bars_sip', 'Alpaca Market Data (SIP)', 'Marché', 'API REST barres 1 minute SIP', 'Export local', 'bronze.raw_bars'),
  ('yahoo_daily', 'Yahoo Finance', 'Marché', 'yfinance : prix quotidiens, dividendes, splits', 'Job Databricks (watermark)', 'bronze.raw_bars')
AS t(source, source_label, source_type, access_mode, ingestion, bronze_table);

CREATE OR REPLACE VIEW gold.dim_model_version AS
SELECT
  model_name,
  model_version,
  max_by(alias, logged_utc) AS alias,
  max_by(run_id, logged_utc) AS run_id,
  max(logged_utc) AS logged_utc,
  max(CASE WHEN metric = 'holdout_macro_f1' THEN value END) AS holdout_macro_f1,
  count(*) AS n_metrics,
  max(logged_utc) = (SELECT max(logged_utc) FROM gold.fact_model_metrics) AS is_latest
FROM gold.fact_model_metrics
GROUP BY model_name, model_version;

CREATE OR REPLACE VIEW gold.fact_price_daily AS
SELECT
  s.hk_security,
  p.symbol,
  p.trade_date,
  p.open, p.high, p.low, p.close, p.adj_close, p.volume, p.dividends, p.splits
FROM silver.silver_prices_daily p
JOIN gold.dim_security s ON s.symbol = p.symbol;

CREATE OR REPLACE VIEW gold.fact_filing_8k AS
SELECT
  h.hk_filing,
  d.hk_security,
  d.symbol,
  d.name,
  d.sector,
  c.form,
  c.filing_date,
  c.report_date,
  c.acceptance_utc,
  c.items,
  c.items_array,
  coalesce(
    try_element_at(
      filter(
        transform(
          c.items_array,
          x -> try_element_at(
            map('2.02', 'résultats', '1.01', 'fusion-acquisition', '2.01', 'fusion-acquisition',
                '5.02', 'direction', '3.01', 'réglementaire-juridique',
                '4.02', 'réglementaire-juridique', '1.03', 'réglementaire-juridique'), x)),
        y -> y IS NOT NULL),
      1),
    'autre') AS category,
  c.document_url,
  c.is_xbrl
FROM vault.hub_filing h
JOIN (
  SELECT * FROM vault.sat_filing_content
  QUALIFY row_number() OVER (PARTITION BY hk_filing ORDER BY load_ts DESC) = 1
) c ON c.hk_filing = h.hk_filing
JOIN vault.link_filing_security l ON l.hk_filing = h.hk_filing
JOIN gold.dim_security d ON d.hk_security = l.hk_security;
