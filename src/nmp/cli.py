import argparse
import logging
import os

from nmp import jobs


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    p = argparse.ArgumentParser("nmp")
    p.add_argument(
        "--catalog",
        default=os.getenv("NMP_CATALOG", "mnp_dev"),
        help="Catalogue Unity Catalog à utiliser",
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    i = sub.add_parser("ingest")
    i.add_argument("--source", required=True)
    i.add_argument("--lookback-days", type=int, default=3)

    b = sub.add_parser("backfill-bars")
    b.add_argument("--start", required=True, help="Début UTC, ex. 2026-11-06T12:00:00Z")
    b.add_argument("--end", required=True, help="Fin UTC, ex. 2026-11-06T22:00:00Z")

    f = sub.add_parser("backfill-finnhub")
    f.add_argument("--start", required=True, help="Premier jour, ex. 2026-11-16")
    f.add_argument("--end", required=True, help="Dernier jour inclus, ex. 2026-11-19")
    f.add_argument("--pause", type=float, default=45.0, help="Secondes entre deux jours (limite de debit)")

    for name in ("enrich", "car"):
        e = sub.add_parser(name)
        e.add_argument(
            "--lookback-hours",
            type=int,
            default=None,
            help="Fenetre de rappel en heures (defaut : valeur de la fonction)",
        )
    sub.add_parser("train")
    sub.add_parser("anomalies")
    sub.add_parser("granger")
    pr = sub.add_parser("promote")
    pr.add_argument("--model", required=True, choices=["xgboost", "random_forest"])
    pr.add_argument("--version", required=True)
    pr.add_argument("--metric", default="holdout_macro_f1")
    pr.add_argument("--margin", type=float, default=0.01)
    pr.add_argument("--dry-run", action="store_true")
    sub.add_parser("publish")
    sub.add_parser("health")

    a = p.parse_args()

    os.environ["NMP_CATALOG"] = a.catalog

    logging.info("Catalogue Unity Catalog utilisé : %s", a.catalog)

    if a.cmd == "ingest":
        jobs.ingest(a.source, a.lookback_days)
    elif a.cmd == "backfill-bars":
        jobs.backfill_bars(a.start, a.end)
    elif a.cmd == "backfill-finnhub":
        jobs.backfill_finnhub_news(a.start, a.end, a.pause)
    elif a.cmd == "promote":
        import mlflow

        from nmp.ml.registry import promote_if_better

        mlflow.set_registry_uri("databricks-uc")
        promote_if_better(
            f"{a.catalog}.gold.event_magnitude_{a.model}", a.version, a.metric, a.margin, dry_run=a.dry_run
        )
    elif a.cmd in ("anomalies", "granger"):
        from nmp import jobs_model3

        getattr(jobs_model3, a.cmd)()
    elif a.cmd in ("enrich", "car") and a.lookback_hours is not None:
        getattr(jobs, a.cmd)(lookback_hours=a.lookback_hours)
    else:
        getattr(jobs, a.cmd)()


if __name__ == "__main__":
    main()
