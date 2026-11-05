"""Etude d'evenements intraday avec modele de marche: R(titre) = alpha + beta * R(SPY) + erreur.

Convention (anti-biais d'anticipation), avec m0 = minute de publication:
  k = 0        : barre contenant la publication -> EXCLUE (contaminee)
  k = -15..-1  : avant la nouvelle (test de fuite d'information)
  k = 1..15    : reaction immediate ; k = 1..60 : reaction etendue
Le modele (alpha, beta) est estime UNIQUEMENT sur des donnees anterieures a m0 - 15 min.
"""

import numpy as np
import pandas as pd
from scipy import stats

MIN = pd.Timedelta(minutes=1)


def log_returns(close: pd.Series) -> pd.Series:
    """Log-rendements sur minutes strictement consecutives (retire trous et rendements overnight)."""
    r = np.log(close).diff()
    r[close.index.to_series().diff() != MIN] = np.nan
    return r


def estimate_market_model(r_stock, r_mkt, est_minutes=390, min_obs=60) -> dict:
    d = pd.concat([r_mkt.rename("m"), r_stock.rename("s")], axis=1).dropna().tail(est_minutes)
    if len(d) < min_obs:
        raise ValueError(f"Fenetre d'estimation trop courte: {len(d)} observations")
    res = stats.linregress(d["m"], d["s"])
    resid = d["s"] - (res.intercept + res.slope * d["m"])
    return {
        "alpha": float(res.intercept),
        "beta": float(res.slope),
        "sigma": float(resid.std(ddof=2)),
        "n_est": len(d),
        "r2": float(res.rvalue**2),
    }


def _window_returns(cs, cm, m0, pre, post) -> pd.DataFrame:
    idx = pd.date_range(m0 - (pre + 1) * MIN, m0 + post * MIN, freq="min")
    # ffill limite a 15 min: une minute sans transaction = prix inchange, pas un trou de donnees
    cs = cs.reindex(idx, method="ffill", tolerance=15 * MIN)
    cm = cm.reindex(idx, method="ffill", tolerance=15 * MIN)
    r_s, r_m = np.log(cs).diff().iloc[1:], np.log(cm).diff().iloc[1:]
    k = ((r_s.index - m0) / MIN).astype(int)
    return pd.DataFrame({"k": k, "r_stock": r_s.values, "r_mkt": r_m.values}, index=r_s.index)


def compute_event(
    bars_s: pd.DataFrame, bars_m: pd.DataFrame, pub_utc, pre=15, post=60, est_minutes=390
) -> tuple[dict, pd.DataFrame]:
    """bars_*: colonnes ts_utc, close, volume (un seul ticker chacun). Retourne (resume, fenetre)."""
    pub = pd.Timestamp(pub_utc).tz_convert("UTC")
    m0 = pub.floor("min")
    cs = bars_s.set_index("ts_utc")["close"].sort_index()
    cm = bars_m.set_index("ts_utc")["close"].sort_index()
    est_end = m0 - (pre + 1) * MIN  # rien de plus recent pour l'estimation
    mm = estimate_market_model(log_returns(cs.loc[:est_end]), log_returns(cm.loc[:est_end]), est_minutes)
    w = _window_returns(cs.loc[: m0 + post * MIN], cm.loc[: m0 + post * MIN], m0, pre, post)
    w["ar"] = w["r_stock"] - (mm["alpha"] + mm["beta"] * w["r_mkt"])
    w.loc[w["k"] == 0, "ar"] = np.nan

    def car(lo, hi):
        sel = w.loc[(w["k"] >= lo) & (w["k"] <= hi), "ar"].dropna()
        c, n = float(sel.sum()), len(sel)
        if n == 0:
            return c, n, np.nan, np.nan
        t = c / (mm["sigma"] * np.sqrt(n))
        return c, n, float(t), float(2 * stats.t.sf(abs(t), mm["n_est"] - 2))

    pre_c, _pre_n, pre_t, pre_p = car(-pre, -1)
    c15, n15, t15, p15 = car(1, 15)
    c60, n60, t60, p60 = car(1, post)
    vol = bars_s.set_index("ts_utc")["volume"].sort_index()
    v_est = vol.loc[:est_end].tail(est_minutes).mean()  # approximation: minutes avec barre seulement
    v_15 = vol.loc[m0 + MIN : m0 + 15 * MIN].mean()
    w["car"] = w["ar"].where(w["k"] >= 1).cumsum()
    summary = {
        "m0_utc": m0,
        "alpha": mm["alpha"],
        "beta": mm["beta"],
        "sigma": mm["sigma"],
        "n_est": mm["n_est"],
        "r2": mm["r2"],
        "car_pre": pre_c,
        "t_pre": pre_t,
        "p_pre": pre_p,
        "car_0_15": c15,
        "n_0_15": n15,
        "t_0_15": t15,
        "p_0_15": p15,
        "car_0_60": c60,
        "n_0_60": n60,
        "t_0_60": t60,
        "p_0_60": p60,
        "rel_volume_15": float(v_15 / v_est) if v_est and not np.isnan(v_15) else np.nan,
        "leak_flag": bool(pre_p < 0.05) if not np.isnan(pre_p) else False,
    }
    return summary, w.reset_index(names="ts_utc")


def cross_sectional_test(cars) -> dict:
    """Test sur l'ensemble des evenements: CAR moyen different de 0? (t-test + Wilcoxon)."""
    s = pd.Series(cars, dtype=float).dropna()
    t, p = stats.ttest_1samp(s, 0.0)
    wp = stats.wilcoxon(s).pvalue if len(s) > 5 else np.nan
    return {"n": len(s), "mean_car": s.mean(), "t_stat": t, "p_ttest": p, "p_wilcoxon": wp}
