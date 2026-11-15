USE CATALOG IDENTIFIER(:catalog);

-- Remplie par scripts/feature_importance.py (importance par permutation du champion).
CREATE TABLE IF NOT EXISTS gold.fact_feature_importance (
  feature STRING, importance_mean DOUBLE, importance_std DOUBLE,
  model_name STRING, model_version STRING, n_test INT, scoring STRING, computed_utc TIMESTAMP
);
