"""Étude d'événements en LOT : une ligne de résultat par (article, titre), plus la fenêtre détaillée.

C'est le maillon qui manquait au pipeline : il transforme « premières publications en séance » + « barres 1 min »
en CAR, t, p-value et test de fuite d'information (`leak_flag`), puis au format de la table bv_event_car.
Pur pandas : testé sans Spark. Le job Databricks `jobs.car` ne fait que lire et écrire.
"""

from __future__ import annotations

import pandas as pd

from nmp import __version__ as CODE_VERSION
from nmp.transform.event_study import compute_event

BENCHMARK = "SPY"
WINDOWS = ((15, "car_0_15", "t_0_15", "p_0_15"), (60, "car_0_60", "t_0_60", "p_0_60"))


def run_batch(
    news: pd.DataFrame,
    bars: pd.DataFrame,
    benchmark: str = BENCHMARK,
    *,
    id_col: str = "article_id",
    ticker_col: str = "ticker",
    ts_col: str = "published_utc",
    keep: tuple[str, ...] = (),
):
    """news : premières publications EN SÉANCE (is_first, session 'pendant'). bars : (ticker, ts_utc, close, volume), SPY inclus.

    Retourne (résultats, fenêtres, erreurs). Les données manquantes sont journalisées dans `erreurs`, jamais fatales.
    """
    by = {t: g.sort_values("ts_utc") for t, g in bars.groupby("ticker")}
    results, windows, errors = [], [], []
    for row in news.to_dict("records"):
        key, tic = row[id_col], row[ticker_col]
        try:
            summary, w = compute_event(by[tic], by[benchmark], row[ts_col])
        except (KeyError, ValueError) as e:
            errors.append({id_col: key, "error": f"{type(e).__name__}: {e}"[:150]})
            continue
        results.append(
            {
                id_col: key,
                ticker_col: tic,
                ts_col: row[ts_col],
                **{c: row[c] for c in keep},
                **summary,
                "code_version": CODE_VERSION,
                "computed_at": pd.Timestamp.now(tz="UTC"),
            }
        )
        windows.append(w.assign(**{id_col: key, ticker_col: tic}))
    all_w = pd.concat(windows, ignore_index=True) if windows else pd.DataFrame()
    return pd.DataFrame(results), all_w, pd.DataFrame(errors, columns=[id_col, "error"])


def to_bv_event_car(res: pd.DataFrame) -> pd.DataFrame:
    """Résultats de run_batch(keep=('hk_security',), id_col='hk_article') -> lignes de business_vault.bv_event_car.

    Deux lignes par (article, titre) : fenêtres [1, 15] et [1, 60] ; la minute de publication (k = 0) est exclue.
    """
    parts = []
    for end, car, t, p in WINDOWS:
        parts.append(
            pd.DataFrame(
                {
                    "hk_article": res["hk_article"],
                    "hk_security": res["hk_security"],
                    "window_start_min": 1,
                    "window_end_min": end,
                    "car": res[car],
                    "t_stat": res[t],
                    "p_value": res[p],
                    "alpha": res["alpha"],
                    "beta": res["beta"],
                    "n_est": res["n_est"],
                    "car_pre": res["car_pre"],
                    "leak_flag": res["leak_flag"],
                    "model_version": f"market-model-{CODE_VERSION}",
                    "load_ts": res["computed_at"],
                }
            )
        )
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
