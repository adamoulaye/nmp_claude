import re
from pathlib import Path


def test_bv_anomaly_ddl_declares_computed_utc():
    sql_file = Path(__file__).resolve().parents[1] / "sql" / "02_business_vault" / "020_business_vault.sql"
    sql = sql_file.read_text(encoding="utf-8")
    block = re.search(r"CREATE TABLE IF NOT EXISTS business_vault\.bv_anomaly \((.*?)\);", sql, re.S).group(1)
    assert "computed_utc TIMESTAMP" in block
