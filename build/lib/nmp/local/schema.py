"""Schemas communs (noms de colonnes). Tous les horodatages sont en UTC."""

NEWS_COLS = [
    "article_id",
    "source",
    "publisher",
    "ticker",
    "published_utc",  # moment de publication (le signal)
    "updated_utc",  # derniere mise a jour par la source
    "ingested_utc",  # moment ou NOTRE systeme l'a recu
    "headline",
    "summary",
    "url",
]
BAR_COLS = ["ticker", "ts_utc", "open", "high", "low", "close", "volume", "trade_count", "vwap"]
