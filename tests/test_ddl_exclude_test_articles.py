from pathlib import Path

FILTRE = "coalesce(source_name, '') <> 'test'"


def test_vue_article_latest_exclut_les_tests():
    for f in ("sql/01_vault/009_vault_schema.sql", "sql/01_vault/010_raw_vault.sql"):
        ligne = next(x for x in Path(f).read_text(encoding="utf-8").splitlines() if "v_article_latest" not in x and "rn = 1" in x)
        assert FILTRE in ligne, f


def test_index_recherche_exclut_les_tests():
    assert FILTRE in Path("sql/03_gold/033_gold_search_index.sql").read_text(encoding="utf-8")
