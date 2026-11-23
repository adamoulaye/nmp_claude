"""
Temps réel : Alpaca (news + barres 1 min) -> Kafka / Azure Event Hubs.

Usage :
    nmp-stream news
    nmp-stream bars

Un processus par flux.
Reconnexion automatique avec backoff exponentiel.
Les doublons sont neutralisés en aval par clé d'idempotence.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone

from nmp.common.config import get_settings

log = logging.getLogger("nmp.streamer")


# ============================================================
# KAFKA PRODUCER
# ============================================================


def _producer(s):
    from confluent_kafka import Producer

    conf = {
        "bootstrap.servers": s.kafka_bootstrap,
        "linger.ms": 50,
        "compression.type": "none",
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10,
    }

    # Azure Event Hubs (API Kafka)
    if s.kafka_password:
        conf.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "$ConnectionString",
                "sasl.password": s.kafka_password,
            }
        )

    return Producer(conf)


# ============================================================
# SERIALIZATION
# ============================================================


def _to_dict(msg) -> dict:

    if hasattr(msg, "model_dump"):
        return msg.model_dump(mode="json")

    if hasattr(msg, "dict"):
        return msg.dict()

    if isinstance(msg, dict):
        return msg

    return dict(msg)


# ============================================================
# KAFKA DELIVERY CALLBACK
# ============================================================


def _delivery_report(err, message):

    if err is not None:
        log.error(
            "Échec publication Kafka : topic=%s erreur=%s",
            message.topic(),
            err,
        )

    else:
        log.info(
            "Message publié : topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )


# ============================================================
# STREAM PROCESSOR
# ============================================================


def run(kind: str) -> None:

    s = get_settings()

    prod = _producer(s)

    topics = {
        "news": "raw.news",
        "bars": "raw.bars",
    }

    if kind not in topics:
        raise ValueError(f"Type de flux inconnu : {kind}")

    topic = topics[kind]

    # --------------------------------------------------------
    # ALPACA HANDLER
    # IMPORTANT : Alpaca exige une coroutine (async def).
    # --------------------------------------------------------

    async def handler(msg):
        log.warning("BAR RECUE : %s", msg)

        key = None
        payload = None

        try:
            # Conversion message Alpaca -> dict
            d = _to_dict(msg)

            log.info("MESSAGE RECU : %s", d)

            # Horodatage UTC de première réception
            d["_received_utc"] = datetime.now(timezone.utc).isoformat()

            # Clé d'idempotence
            key = str(d.get("id") or f"{d.get('symbol')}:{d.get('timestamp')}")

            # Sérialisation JSON
            payload = json.dumps(
                d,
                default=str,
                ensure_ascii=False,
            )

            # Publication Kafka
            prod.produce(
                topic,
                key=key.encode("utf-8"),
                value=payload.encode("utf-8"),
                callback=_delivery_report,
            )

            # Traitement des callbacks Kafka
            prod.poll(0)

            log.info(
                "SEND -> %s | key=%s",
                topic,
                key,
            )

        except BufferError:
            log.warning("Buffer Kafka saturé : attente de capacité disponible")

            # Attendre la libération du buffer
            prod.poll(1)

            # Nouvelle tentative
            try:
                prod.produce(
                    topic,
                    key=key.encode("utf-8"),
                    value=payload.encode("utf-8"),
                    callback=_delivery_report,
                )

                prod.poll(0)

            except Exception:
                log.exception("Échec nouvelle tentative Kafka")

        except Exception:
            log.exception(
                "Erreur traitement message %s",
                kind,
            )

    # --------------------------------------------------------
    # RECONNECTION LOOP
    # --------------------------------------------------------

    backoff = 1

    while True:
        st = None

        try:
            # =================================================
            # NEWS STREAM
            # =================================================

            if kind == "news":
                from alpaca.data.live import NewsDataStream

                st = NewsDataStream(
                    s.alpaca_key,
                    s.alpaca_secret,
                )

                st.subscribe_news(
                    handler,
                    "*",
                )

            # =================================================
            # BARS STREAM
            # =================================================

            else:
                from alpaca.data.enums import DataFeed
                from alpaca.data.live import StockDataStream

                st = StockDataStream(  # type: ignore[assignment]
                    s.alpaca_key,
                    s.alpaca_secret,
                    feed=DataFeed(s.alpaca_feed),
                )

                st.subscribe_bars(  # type: ignore[union-attr]
                    handler,
                    *s.bar_tickers,
                )

            log.info(
                "Flux %s démarré",
                kind,
            )

            # Réinitialisation du backoff après démarrage
            backoff = 1

            # Boucle bloquante Alpaca
            st.run()  # type: ignore[union-attr]

            log.warning(
                "Flux %s arrêté",
                kind,
            )

        except KeyboardInterrupt:
            log.info("Arrêt demandé par utilisateur")

            break

        except Exception:
            log.exception(
                "Flux %s interrompu, reprise dans %ss",
                kind,
                backoff,
            )

        finally:
            # Arrêt du flux Alpaca
            if st is not None:
                try:
                    st.stop()

                except Exception:
                    log.debug(
                        "Arrêt du flux déjà effectué",
                        exc_info=True,
                    )

            # Vidage du producteur Kafka
            remaining = prod.flush(5)

            if remaining:
                log.warning(
                    "%s messages Kafka encore en attente",
                    remaining,
                )

        # Attente avant reconnexion
        time.sleep(backoff)

        # Backoff exponentiel plafonné à 60 secondes
        backoff = min(
            backoff * 2,
            60,
        )

    # Vidage final Kafka
    remaining = prod.flush(10)

    if remaining:
        log.warning(
            "%s messages non livrés à l'arrêt",
            remaining,
        )


# ============================================================
# CLI ENTRYPOINT
# ============================================================


def main() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format=("%(asctime)s %(levelname)s %(name)s %(message)s"),
    )

    if len(sys.argv) != 2 or sys.argv[1] not in ("news", "bars"):
        sys.exit("usage: nmp-stream news|bars")

    run(sys.argv[1])


if __name__ == "__main__":
    main()
