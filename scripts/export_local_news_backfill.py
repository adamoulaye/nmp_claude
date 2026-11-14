import hashlib
import json
from pathlib import Path

import pandas as pd
from nmp.local.delta_io import read_delta

BRONZE = Path("data/bronze")
OUTPUT = Path("data/exports/local_news_backfill.csv")


def payload_hash(payload):
    raw = json.dumps(payload, sort_keys=True, default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def iso(value):
    if value is None:
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    return None if pd.isna(ts) else ts.isoformat()


def main():
    car = read_delta("event_results_batch")
    targets = set()

    for r in car[["article_id", "ticker", "source"]].drop_duplicates().itertuples(index=False):
        source = str(r.source).lower()
        article_id = str(r.article_id).split(":", 1)[-1]
        ticker = str(r.ticker).upper().strip()
        targets.add((source, article_id, ticker))

    rows = {}

    for path in BRONZE.rglob("*.json"):
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        source_name = obj.get("source")
        if source_name not in {"alpaca_news", "finnhub"}:
            continue

        ingested = obj.get("ingested_at")
        payload = obj.get("payload") or []

        if source_name == "alpaca_news":
            articles = [
                (a, None)
                for page in payload if isinstance(page, dict)
                for a in (page.get("news") or [])
            ]
            source = "alpaca"
        else:
            source = "finnhub"
            ticker = str(obj.get("key") or "").upper()
            articles = [(a, ticker) for a in payload if isinstance(a, dict)]

        for article, ticker in articles:
            aid = str(article.get("id", ""))
            if source == "finnhub":
                if (source, aid, ticker) not in targets:
                    continue
                normalized = dict(article)
                normalized["_symbol"] = ticker
                record_id = f"{aid}:{ticker}"
                symbols = {ticker}
                symbols.update(
                    x.strip().upper()
                    for x in str(article.get("related") or "").split(",")
                    if x.strip()
                )
                published = pd.to_datetime(
                    article.get("datetime"), unit="s", utc=True, errors="coerce"
                )
                updated = None
            else:
                if not any(s == source and a == aid for s, a, _ in targets):
                    continue
                normalized = article
                record_id = aid
                symbols = {
                    str(x).strip().upper()
                    for x in article.get("symbols", [])
                    if str(x).strip()
                }
                published = pd.to_datetime(
                    article.get("created_at"), utc=True, errors="coerce"
                )
                updated = pd.to_datetime(
                    article.get("updated_at"), utc=True, errors="coerce"
                )

            if pd.isna(published):
                continue

            digest = payload_hash(normalized)
            key = (source, aid, digest)
            rows[key] = {
                "source": f"{source}_news",
                "article_id": aid,
                "headline": article.get("headline"),
                "summary": article.get("summary") or "",
                "source_name": article.get("source"),
                "url": article.get("url"),
                "symbols": json.dumps(sorted(symbols)),
                "published_utc": iso(published),
                "updated_utc": iso(updated),
                "first_seen_utc": iso(ingested),
                "ingested_utc": iso(ingested),
                "record_hash": digest,
                "bronze_ref": f"{source}_news:{record_id}:{digest}",
            }

    df = pd.DataFrame(rows.values())
    expected = {(s, a) for s, a, _ in targets}
    actual = {
        ("alpaca" if r.source == "alpaca_news" else "finnhub", r.article_id)
        for r in df.itertuples(index=False)
    }
    missing = expected - actual

    print("Résultats CAR :", len(car))
    print("Associations article-ticker :", len(targets))
    print("Articles attendus :", len(expected))
    print("Articles exportés :", len(actual))
    print("Articles manquants :", len(missing))
    if missing:
        print("Exemples manquants :", sorted(missing)[:10])
        raise RuntimeError("Export incomplet : aucun CSV final n'est écrit.")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.sort_values(["source", "article_id"]).to_csv(OUTPUT, index=False)
    print("Lignes exportées :", len(df))
    print("Fichier :", OUTPUT)
    print("Taille :", OUTPUT.stat().st_size, "octets")


if __name__ == "__main__":
    main()
