-- CHARGEMENT MANUEL (hors pipeline) : barres SIP 1 min -> vault.nhsat_bar_1m. Aucun job ne lance ce script.
-- (1) python scripts/export_bars.py --feed sip --symbols <liste> --days <n> --out bars_sip.jsonl
-- (2) deposer bars_sip.jsonl dans le Volume /Volumes/mnp_dev/bronze/landing/
-- (3) executer ce script a la main sur l'entrepot dev (MERGE : seance reguliere 9h30-16h, heure de New York)
-- Effet : les barres deja presentes (flux IEX) sont ECRASEES par les valeurs SIP ; les nouvelles sont inserees.
-- Cible : vault.nhsat_bar_1m, record_source = 'alpaca_bars_sip' (ecrit directement dans le Vault, sans passer par le Silver).
MERGE INTO vault.nhsat_bar_1m t
USING (
  SELECT sha2(upper(trim(j._symbol)), 256) AS hk_security,
         to_timestamp(j.t) AS bar_ts,
         j.o AS open, j.h AS high, j.l AS low, j.c AS close,
         j.v AS volume, j.vw AS vwap, j.n AS trade_count,
         current_timestamp() AS load_ts,
         'alpaca_bars_sip' AS record_source
  FROM (
    SELECT from_json(value, 't STRING, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, vw DOUBLE, n DOUBLE, _symbol STRING') AS j
    FROM read_files('/Volumes/mnp_dev/bronze/landing/bars_sip.jsonl', format => 'text')
  )
  WHERE j._symbol IS NOT NULL AND j.t IS NOT NULL
    AND hour(from_utc_timestamp(to_timestamp(j.t), 'America/New_York')) * 60
        + minute(from_utc_timestamp(to_timestamp(j.t), 'America/New_York')) >= 570
    AND hour(from_utc_timestamp(to_timestamp(j.t), 'America/New_York')) * 60
        + minute(from_utc_timestamp(to_timestamp(j.t), 'America/New_York')) < 960
  QUALIFY row_number() OVER (PARTITION BY j._symbol, j.t ORDER BY j.t) = 1
) s
ON t.hk_security = s.hk_security AND t.bar_ts = s.bar_ts
WHEN MATCHED THEN UPDATE SET
  t.open = s.open, t.high = s.high, t.low = s.low, t.close = s.close,
  t.volume = s.volume, t.vwap = s.vwap, t.trade_count = s.trade_count,
  t.load_ts = s.load_ts, t.record_source = s.record_source
WHEN NOT MATCHED THEN INSERT *
