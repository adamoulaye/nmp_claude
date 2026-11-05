from nmp.local.search_index import build_index, search
from nmp.transform.category import categorize, categorize_8k


def test_categorize():
    assert categorize("Apple beats earnings estimates") == "résultats"
    assert categorize("Tesla CEO steps down") == "direction"
    assert categorize("Pfizer to acquire biotech firm") == "fusion-acquisition"
    assert categorize("Something unrelated") == "autre"
    assert categorize_8k("2.02,9.01") == "résultats" and categorize_8k("9.01") == "autre"


def test_fulltext_search(tmp_path):
    db = str(tmp_path / "idx.db")
    build_index(
        db,
        [
            ("a1", "AAPL", "Apple beats earnings", "strong iPhone sales"),
            ("a2", "TSLA", "Tesla recalls vehicles", "software update"),
        ],
    )
    assert [r[0] for r in search(db, "earnings")] == ["a1"]
    assert [r[0] for r in search(db, "recall")] == ["a2"]  # racinisation (porter)
    assert search(db, "earnings", ticker="TSLA") == []
