import argparse
import logging

from nmp import jobs


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser("nmp")
    sub = p.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("ingest")
    i.add_argument("--source", required=True)
    i.add_argument("--lookback-days", type=int, default=3)
    sub.add_parser("enrich")
    sub.add_parser("car")
    sub.add_parser("train")
    sub.add_parser("health")
    a = p.parse_args()
    if a.cmd == "ingest":
        jobs.ingest(a.source, a.lookback_days)
    else:
        getattr(jobs, a.cmd)()


if __name__ == "__main__":
    main()
