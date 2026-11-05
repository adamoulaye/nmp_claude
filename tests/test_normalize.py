from nmp.local.sources import alpaca, finnhub


def test_finnhub_normalize():
    items = [{"id": 1, "datetime": 1760364000, "headline": "H", "source": "Reuters", "url": "u"}]
    df = finnhub.normalize_news(items, "AAPL")
    assert df.loc[0, "article_id"] == "finnhub:1"
    assert str(df.loc[0, "published_utc"]) == "2025-10-13 14:00:00+00:00"


def test_alpaca_news_explodes_symbols_and_bars_dedupe():
    pages = [
        {
            "news": [
                {
                    "id": 7,
                    "created_at": "2026-10-13T14:00:05Z",
                    "updated_at": "2026-10-13T14:01:00Z",
                    "symbols": ["AAPL", "MSFT"],
                    "headline": "H",
                    "source": "benzinga",
                    "url": "u",
                }
            ]
        }
    ]
    assert len(alpaca.normalize_news(pages)) == 2
    assert len(alpaca.normalize_news(pages, tickers=["AAPL"])) == 1
    bar = {"t": "2026-10-13T14:00:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 10, "n": 3, "vw": 1.5}
    bars = alpaca.normalize_bars([{"bars": {"AAPL": [bar, bar]}}, {"bars": None}])
    assert len(bars) == 1
