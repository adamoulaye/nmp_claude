"""Téléchargement historique Yahoo Finance vers Bronze local."""

import argparse
from datetime import date, datetime, timezone

from nmp.local.bronze import log_run, save_raw
from nmp.local.config import BENCHMARK, PROJECT_TICKERS
from nmp.local.sources import yahoo


def archive_job(source, symbol, fetch, params=None):
    started = datetime.now(timezone.utc)

    try:
        payload = fetch()

        save_raw(
            source=source,
            key=symbol,
            payload=payload,
            params=params,
        )

        count = len(payload) if isinstance(payload, (list, dict)) else 1

        log_run(
            source=source,
            key=symbol,
            n_received=count,
            started=started,
            status="ok",
        )

        print(f"OK  {source:15s} {symbol:6s} : {count} enregistrement(s)")
        return payload

    except Exception as exc:
        log_run(
            source=source,
            key=symbol,
            n_received=0,
            started=started,
            status="error",
            error=str(exc),
        )

        print(f"ERREUR {source:15s} {symbol:6s} : {exc}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Backfill historique Yahoo Finance vers Bronze."
    )

    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end", default=date.today().isoformat())

    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    if end < start:
        raise ValueError("La date de fin doit être postérieure à la date de début.")

    symbols = list(dict.fromkeys(PROJECT_TICKERS + [BENCHMARK]))

    print("=" * 65)
    print("BACKFILL YAHOO FINANCE")
    print(f"Période : {start} au {end}")
    print(f"Actions : {len(PROJECT_TICKERS)}")
    print(f"Benchmark : {BENCHMARK}")
    print(f"Total de symboles : {len(symbols)}")
    print("=" * 65)

    # Historique quotidien pour les 20 actions et le benchmark.
    for symbol in symbols:
        archive_job(
            source="yahoo_daily",
            symbol=symbol,
            fetch=lambda s=symbol: yahoo.fetch_daily(s, start, end),
            params={
                "start": str(start),
                "end": str(end),
                "auto_adjust": False,
                "actions": True,
            },
        )

    # Métadonnées sectorielles des 20 actions.
    sectors = {}

    for symbol in PROJECT_TICKERS:
        profile = archive_job(
            source="yahoo_reference",
            symbol=symbol,
            fetch=lambda s=symbol: yahoo.fetch_profile(s),
            params={"fields": ["name", "sector", "industry", "currency", "exchange"]},
        )

        if profile and profile.get("sector"):
            sectors[symbol] = profile["sector"]

    print("\nRÉCAPITULATIF DES SECTEURS")
    for symbol, sector in sorted(sectors.items()):
        print(f"{symbol:6s} : {sector}")

    distinct_sectors = set(sectors.values())

    print(f"\nActions avec secteur identifié : {len(sectors)}/{len(PROJECT_TICKERS)}")
    print(f"Nombre de secteurs distincts : {len(distinct_sectors)}")

    if len(distinct_sectors) < 3:
        print("ATTENTION : moins de trois secteurs distincts ont été identifiés.")

    print("\nBackfill terminé. Consulter data/logs/runs.jsonl pour les résultats.")


if __name__ == "__main__":
    main()
