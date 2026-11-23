"""Quarantaine : l'enonce demande une table de quarantaine cote Silver (enregistrements refuses a l'ingestion)."""

import re
from pathlib import Path

SQL = Path(__file__).resolve().parents[1] / "sql" / "00_ops" / "001_schemas_ops.sql"


def test_silver_quarantine_exposes_bronze_rejects():
    ddl = SQL.read_text(encoding="utf-8")
    m = re.search(r"CREATE OR REPLACE VIEW silver\.quarantine AS(.*?);", ddl, re.S)
    assert m, "silver.quarantine absent de 001_schemas_ops.sql"
    assert "FROM bronze.rejects" in m.group(1)
    assert ddl.index("bronze.rejects (") < ddl.index("silver.quarantine")  # la table existe avant la vue
