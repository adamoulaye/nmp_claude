from pathlib import Path


def test_model3_ignore_les_articles_de_test():
    src = Path("src/nmp/jobs_model3.py").read_text(encoding="utf-8")
    assert "coalesce(a.source_name, '') <> 'test'" in src
