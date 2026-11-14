import json, os, time
from datetime import datetime, timezone
from confluent_kafka import Producer

for line in open(".env", encoding="utf-8"):
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.split("#", 1)[0].strip().strip('"').strip("'"))

p = Producer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString", "sasl.password": os.environ["KAFKA_SASL_PASSWORD"],
})
now = datetime.now(timezone.utc).isoformat()
msg = {"id": int(time.time()), "headline": "TEST-demo : AAPL annonce un test de flux",
       "summary": "message de test", "source": "test", "url": "https://example.com/test",
       "symbols": ["AAPL"], "created_at": now, "updated_at": now, "_received_utc": now}
p.produce("raw.news", key=str(msg["id"]), value=json.dumps(msg))
print("restants non livrés :", p.flush(15))
