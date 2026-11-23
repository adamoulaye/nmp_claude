from pathlib import Path

SQL = Path("sql/04_ops/040_slo_status.sql").read_text(encoding="utf-8")


def test_s7_lit_ops_api_quota():
    line = next(x for x in SQL.splitlines() if "'S7" in x)
    assert "FROM ops.api_quota" in line
    assert "CASE WHEN max(quota) IS NULL THEN 'NON_INSTRUMENTE'" in line
