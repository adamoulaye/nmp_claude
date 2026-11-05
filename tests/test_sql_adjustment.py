"""La vue SQL v_adjustment_factor doit donner EXACTEMENT le calcul pandas de référence (transform/adjust.py), sans doublon.

Exécutée avec DuckDB (deux fonctions Spark remplacées : to_date(from_utc_timestamp(..)) -> CAST). Non exécutée sur Databricks.
"""

import pathlib
import re

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")
from nmp.transform.adjust import adjustment_factor  # noqa: E402

SQL = pathlib.Path(__file__).parents[1] / "sql/02_business_vault/020_business_vault.sql"
DAYS = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"])
CASES = {  # titre -> (clôtures brutes, splits {indice: ratio}, dividendes {indice: montant})
    "A": ([100, 100, 50, 50, 50], {2: 2.0}, {3: 1.0}),
    "B": ([80, 82, 84, 43, 44], {3: 2.0}, {3: 0.86}),  # split ET dividende le même jour
    "C": ([10, 11, 12, 13, 14], {}, {}),
}


def _view_sql() -> str:
    s = SQL.read_text()
    v = s[s.index("CREATE OR REPLACE VIEW business_vault.v_adjustment_factor AS") : s.index("-- Table PIT")]
    return re.sub(r"to_date\(from_utc_timestamp\(bar_ts, 'America/New_York'\)\)", "CAST(bar_ts AS DATE)", v)


def test_vue_sql_egale_reference_pandas():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA vault; CREATE SCHEMA business_vault;")
    con.execute("CREATE TABLE vault.nhsat_bar_1m (hk_security VARCHAR, bar_ts TIMESTAMP, close DOUBLE)")
    con.execute("CREATE TABLE vault.sat_corporate_action (hk_security VARCHAR, ex_date DATE, action VARCHAR, value DOUBLE)")
    for k, (cl, sp, dv) in CASES.items():
        for d, c in zip(DAYS, cl, strict=True):
            con.execute("INSERT INTO vault.nhsat_bar_1m VALUES (?,?,?)", [k, d + pd.Timedelta(hours=15), float(c)])
        for i, v in sp.items():
            con.execute("INSERT INTO vault.sat_corporate_action VALUES (?,?,'split',?)", [k, DAYS[i].date(), v])
        for i, v in dv.items():
            con.execute("INSERT INTO vault.sat_corporate_action VALUES (?,?,'dividend',?)", [k, DAYS[i].date(), v])
    con.execute(_view_sql())
    res = con.execute("SELECT * FROM business_vault.v_adjustment_factor ORDER BY hk_security, d").df()
    assert len(res) == 5 * len(CASES)  # une ligne par (titre, séance) : aucun doublon de jointure
    idx = [d.date() for d in DAYS]
    for k, (cl, sp, dv) in CASES.items():
        close = pd.Series(cl, index=idx, dtype=float)
        splits = pd.Series({idx[i]: v for i, v in sp.items()}, dtype=float)
        divs = pd.Series({idx[i]: v for i, v in dv.items()}, dtype=float)
        g = res[res.hk_security == k]
        assert g["adj_factor"].to_numpy() == pytest.approx(adjustment_factor(close, splits, divs).to_numpy())
        assert g["split_factor"].to_numpy() == pytest.approx(adjustment_factor(close, splits).to_numpy())
