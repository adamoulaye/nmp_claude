"""Index de recherche plein texte minimal (SQLite FTS5, bibliotheque standard de Python).
On indexe titre + resume SEULEMENT; les rapports n'affichent jamais le texte integral."""

import sqlite3


def build_index(db_path: str, rows: list[tuple[str, str, str, str]]) -> None:
    """rows: (article_id, ticker, headline, summary)."""
    con = sqlite3.connect(db_path)
    con.execute("DROP TABLE IF EXISTS news_fts")
    con.execute(
        "CREATE VIRTUAL TABLE news_fts USING fts5(article_id UNINDEXED, ticker UNINDEXED,"
        " headline, summary, tokenize='porter unicode61')"
    )
    con.executemany("INSERT INTO news_fts VALUES (?,?,?,?)", rows)
    con.commit()
    con.close()


def search(db_path: str, query: str, ticker: str | None = None, limit: int = 20):
    """Retourne [(article_id, ticker, headline)] classes par pertinence (bm25)."""
    sql = (
        "SELECT article_id, ticker, headline FROM news_fts WHERE news_fts MATCH ?"
        + (" AND ticker = ?" if ticker else "")
        + " ORDER BY bm25(news_fts) LIMIT ?"
    )
    args = [query] + ([ticker] if ticker else []) + [limit]
    con = sqlite3.connect(db_path)
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()
