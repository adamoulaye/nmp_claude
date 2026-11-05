"""Calcule le CAR de TOUTES les nouvelles archivées (Bronze) publiées en séance. Rejouable, sans doublons.
Prérequis : python scripts/backfill.py --start ... --end ...
Usage : python scripts/run_events.py [--start 2026-07-01] [--end 2026-10-23] [--tickers AAPL,TSLA]"""

import argparse
from datetime import date

from nmp.local.config import DATA_DIR, PILOT_TICKERS
from nmp.local.delta_io import to_delta
from nmp.local.events_batch import run, summarize


def main(start, end, tickers):
    res, windows, err, stats = run(DATA_DIR, tickers, start, end)
    print(
        f"Nouvelles lues : {stats['news_lues']} | événements en séance (1res publications) : {stats['evenements_en_seance']} | barres : {stats['barres']}"
    )
    if res.empty:
        raise SystemExit("Aucun événement calculable : vérifier le backfill (data/bronze) et les dates.")
    to_delta(res, "event_results_batch", mode="overwrite")
    to_delta(windows, "event_windows_batch", mode="overwrite")
    out = DATA_DIR / "exports"
    out.mkdir(parents=True, exist_ok=True)
    res.to_csv(out / "event_results_batch.csv", index=False)
    err.to_csv(out / "event_errors.csv", index=False)
    print(f"Calculés : {len(res)} | non calculables : {len(err)} (détail : {out / 'event_errors.csv'})")
    if len(err):
        print("  motifs :", err["error"].str.slice(0, 60).value_counts().head(3).to_dict())
    print("\nPar titre :", res["ticker"].value_counts().to_dict())
    s = summarize(res)
    print(f"Fuites d'information écartées : {s['n_fuite_ecartes']} | testés : {s['n_testes']}")
    for w in ("car_0_15", "car_0_60"):
        if w in s:
            c = s[w]
            print(f"\n{w}: moyenne {c['mean_car']:+.3%} | t-test p = {c['p_ttest']:.3f} | Wilcoxon p = {c['p_wilcoxon']:.3f}")
        print(
            f"  ampleur moyenne |CAR| = {s[w + '_ampleur_moyenne']:.3%} | part des événements p<0,05 = {s[w + '_part_p<0.05']:.0%} (≈5 % attendu par hasard)"
        )
    print(f"\nRésultats : {out / 'event_results_batch.csv'}  et  data/delta/event_results_batch")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=date.fromisoformat)
    ap.add_argument("--end", type=date.fromisoformat)
    ap.add_argument("--tickers", default=",".join(PILOT_TICKERS))
    a = ap.parse_args()
    main(a.start, a.end, [t.strip() for t in a.tickers.split(",")])
