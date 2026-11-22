from datetime import datetime, timezone

from nmp.ingestion.base import payload_hash
from nmp.ingestion.sources import FinnhubSentiment


def test_finnhub_sentiment_excludes_asof_from_payload():
    source = FinnhubSentiment.__new__(FinnhubSentiment)

    first = {
        "symbol": "MSFT",
        "bullishPercent": 0.65,
        "bearishPercent": 0.20,
        "_asof": "2026-11-09T10:00:00+00:00",
    }

    second = {
        **first,
        "_asof": "2026-11-09T15:00:00+00:00",
    }

    record1 = source.to_record(first)
    record2 = source.to_record(second)

    assert "_asof" not in record1.payload
    assert "_asof" not in record2.payload

    assert record1.record_id == record2.record_id
    assert record1.symbol == "MSFT"
    assert record2.symbol == "MSFT"

    assert payload_hash(record1.payload) == payload_hash(record2.payload)


def test_finnhub_sentiment_detects_content_change():
    source = FinnhubSentiment.__new__(FinnhubSentiment)

    first = {
        "symbol": "MSFT",
        "bullishPercent": 0.65,
        "_asof": "2026-11-09T10:00:00+00:00",
    }

    changed = {
        "symbol": "MSFT",
        "bullishPercent": 0.75,
        "_asof": "2026-11-09T15:00:00+00:00",
    }

    record1 = source.to_record(first)
    record2 = source.to_record(changed)

    assert record1.record_id == record2.record_id
    assert payload_hash(record1.payload) != payload_hash(record2.payload)


def test_finnhub_sentiment_uses_utc_timestamp():
    source = FinnhubSentiment.__new__(FinnhubSentiment)

    raw = {
        "symbol": "AAPL",
        "bullishPercent": 0.70,
        "_asof": "2026-11-09T15:30:00+00:00",
    }

    record = source.to_record(raw)

    assert record.published_utc == datetime(
        2026, 11, 9, 15, 30, tzinfo=timezone.utc
    )
