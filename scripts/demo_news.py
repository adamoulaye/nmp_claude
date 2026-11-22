"""Envoie une nouvelle DEMO- dans raw.news (Event Hubs) et note l'heure d'envoi."""
import json
import os
import sys
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

for line in open(".env", encoding="utf-8"):
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.split("#", 1)[0].strip().strip('"').strip("'"))

text = sys.argv[1] if len(sys.argv) > 1 else "AAPL annonce un test de flux"
dup = len(sys.argv) > 2 and sys.argv[2] == "dup"
symbol = os.environ.get("DEMO_SYMBOL", "AAPL")
p = Producer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString", "sasl.password": os.environ["KAFKA_SASL_PASSWORD"],
})
now = datetime.now(timezone.utc).isoformat()
msg = {"id": int(time.time()), "headline": f"DEMO- {text}", "summary": "message de demonstration", "source": "demo", "url": "https://example.com/demo", "symbols": [symbol], "created_at": now, "updated_at": now, "_received_utc": now}
for _ in range(2 if dup else 1):
    p.produce("raw.news", key=str(msg["id"]), value=json.dumps(msg))
print("restants non livres :", p.flush(15))
json.dump({"id": msg["id"], "sent_utc": now, "headline": msg["headline"], "duplicate": dup}, open("demo_last_sent.json", "w"))
print("envoye :", msg["id"], now, "(doublon)" if dup else "")
