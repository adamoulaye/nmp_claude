"""Temps réel : Alpaca (news + barres 1 min) -> Kafka / Azure Event Hubs. Un processus par flux.

Usage : nmp-stream news | nmp-stream bars
Reconnexion automatique avec backoff ; les doublons sont neutralisés en aval (clé d'idempotence).
"""

from __future__ import annotations

import json
import logging
import sys
import time

from nmp.common.config import get_settings

log = logging.getLogger("nmp.streamer")


def _producer(s):
    from confluent_kafka import Producer

    conf = {
        "bootstrap.servers": s.kafka_bootstrap,
        "linger.ms": 50,
        "compression.type": "lz4",
        "enable.idempotence": True,
    }
    if s.kafka_password:  # Azure Event Hubs (API Kafka)
        conf |= {
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": s.kafka_password,
        }
    return Producer(conf)


def _to_dict(msg) -> dict:
    d = msg.model_dump() if hasattr(msg, "model_dump") else msg.dict() if hasattr(msg, "dict") else dict(msg)
    return d


def run(kind: str) -> None:
    s = get_settings()
    prod = _producer(s)
    topic = {"news": "raw.news", "bars": "raw.bars"}[kind]

    async def handler(msg):
        d = _to_dict(msg)
        d["_received_utc"] = time.time()  # horodatage de 1re vue : base du contrôle anti-anticipation
        key = str(d.get("id") or f"{d.get('symbol')}:{d.get('timestamp')}")
        prod.produce(topic, key=key, value=json.dumps(d, default=str))
        prod.poll(0)

    backoff = 1
    while True:
        try:
            if kind == "news":
                from alpaca.data.live import NewsDataStream

                st = NewsDataStream(s.alpaca_key, s.alpaca_secret)
                st.subscribe_news(handler, "*")
            else:
                from alpaca.data.enums import DataFeed
                from alpaca.data.live import StockDataStream

                st = StockDataStream(s.alpaca_key, s.alpaca_secret, feed=DataFeed(s.alpaca_feed))
                st.subscribe_bars(handler, *s.bar_tickers)
            log.info("stream %s démarré", kind)
            backoff = 1
            st.run()  # bloque jusqu'à déconnexion
        except Exception:
            log.exception("flux %s interrompu, reprise dans %ss", kind, backoff)
        finally:
            prod.flush(5)
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if len(sys.argv) != 2 or sys.argv[1] not in ("news", "bars"):
        sys.exit("usage: nmp-stream news|bars")
    run(sys.argv[1])


if __name__ == "__main__":
    main()
