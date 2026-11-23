"""Dimension secteur du modele en etoile (enonce : dimensions temps, ticker, secteur, source, version de modele)."""

import re
from pathlib import Path

SQL = Path(__file__).resolve().parents[1] / "sql" / "03_gold" / "030_gold.sql"


def test_dim_sector_is_derived_from_dim_security():
    ddl = SQL.read_text(encoding="utf-8")
    m = re.search(r"CREATE OR REPLACE VIEW gold\.dim_sector AS(.*?);", ddl, re.S)
    assert m, "gold.dim_sector absent de 030_gold.sql"
    body = m.group(1)
    assert "FROM gold.dim_security" in body
    assert "GROUP BY sector" in body
    assert "hk_sector" in body
