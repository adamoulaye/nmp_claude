"""Controle de sante: a planifier toutes les 5 minutes. Alerte si une source est muette.
Usage: python scripts/check_slos.py [--ignore-market-hours]   (option utile pour la demo)"""

import argparse
import math

from nmp.local.quality import SLO, load_runs, pipeline_success_rate, send_alert, silent_sources

ap = argparse.ArgumentParser()
ap.add_argument("--ignore-market-hours", action="store_true")
args = ap.parse_args()

runs = load_runs()
for a in silent_sources(runs, ignore_market_hours=args.ignore_market_hours):
    send_alert(f"Source muette: {a['source']} (derniere reussite il y a {a['age_min']} min, limite {a['limit_min']} min)")
rate = pipeline_success_rate(runs)
target = SLO["pipeline_success_rate"]
if not math.isnan(rate) and rate < target:
    send_alert(f"Taux de succes des pipelines (24 h) = {rate:.1%} < {target:.0%}")
