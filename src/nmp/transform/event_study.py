
"""Etude d'evenements intraday avec modele de marche.

Convention :
  k = 0       : barre contenant la publication, exclue
  k = -15..-1 : periode anterieure, test de fuite
  k = 1..15   : reaction immediate
  k = 1..60   : reaction etendue

Le modele est estime uniquement avant m0 - 15 minutes.
Les fenetres CAR exigent des observations reelles pour chaque minute.
"""

import numpy as np
import pandas as pd
from scipy import stats

MIN = pd.Timedelta(minutes=1)


class IncompleteEventError(ValueError):
    """Les donnees intraday ne couvrent pas toute la fenetre."""


def log_returns(close: pd.Series) -> pd.Series:
    """Calcule les log-rendements sur des minutes consecutives."""
    r = np.log(close).diff()
    r[close.index.to_series().diff() != MIN] = np.nan
    return r


def estimate_market_model(
    r_stock,
    r_mkt,
    est_minutes=390,
    min_obs=60,
) -> dict:
    """Estime le modele de marche sur les observations historiques."""
    d = (
        pd.concat(
            [r_mkt.rename("m"), r_stock.rename("s")],
            axis=1,
        )
        .dropna()
        .tail(est_minutes)
    )

    if len(d) < min_obs:
        raise ValueError(
            f"Fenetre d'estimation trop courte : {len(d)} observations"
        )

    res = stats.linregress(d["m"], d["s"])

    resid = d["s"] - (
        res.intercept + res.slope * d["m"]
    )

    sigma = float(resid.std(ddof=2))

    if not np.isfinite(sigma) or sigma <= 0:
        raise ValueError("Volatilite residuelle invalide.")

    return {
        "alpha": float(res.intercept),
        "beta": float(res.slope),
        "sigma": sigma,
        "n_est": len(d),
        "r2": float(res.rvalue**2),
    }


def _window_returns(
    cs,
    cm,
    m0,
    pre,
    post,
) -> pd.DataFrame:
    """Construit les rendements sans inventer les cours manquants."""
    idx = pd.date_range(
        m0 - (pre + 1) * MIN,
        m0 + post * MIN,
        freq="min",
    )

    # Pas de forward-fill : une barre absente reste absente.
    cs_window = cs.reindex(idx)
    cm_window = cm.reindex(idx)

    r_s = np.log(cs_window).diff().iloc[1:]
    r_m = np.log(cm_window).diff().iloc[1:]

    k = ((r_s.index - m0) / MIN).astype(int)

    return pd.DataFrame(
        {
            "k": k,
            "r_stock": r_s.values,
            "r_mkt": r_m.values,
        },
        index=r_s.index,
    )


def compute_event(
    bars_s: pd.DataFrame,
    bars_m: pd.DataFrame,
    pub_utc,
    pre=15,
    post=60,
    est_minutes=390,
) -> tuple[dict, pd.DataFrame]:
    """Calcule un evenement seulement si ses fenetres sont completes."""

    pub = pd.Timestamp(pub_utc)

    if pub.tzinfo is None:
        pub = pub.tz_localize("UTC")
    else:
        pub = pub.tz_convert("UTC")

    m0 = pub.floor("min")

    def prepare_bars(df, ticker):
        required = {"ts_utc", "close", "volume"}
        missing = required.difference(df.columns)

        if missing:
            raise ValueError(
                f"Colonnes manquantes pour {ticker} : {sorted(missing)}"
            )

        out = df.copy()

        out["ts_utc"] = pd.to_datetime(
            out["ts_utc"],
            utc=True,
            errors="coerce",
        )

        out["close"] = pd.to_numeric(
            out["close"],
            errors="coerce",
        )

        out["volume"] = pd.to_numeric(
            out["volume"],
            errors="coerce",
        )

        out = out.dropna(subset=["ts_utc", "close"])
        out = out[out["close"] > 0]
        out = out.sort_values("ts_utc")
        out = out.drop_duplicates("ts_utc", keep="last")

        if out.empty:
            raise ValueError(f"Aucune barre valide pour {ticker}")

        return out.set_index("ts_utc")

    stock = prepare_bars(bars_s, "titre")
    market = prepare_bars(bars_m, "benchmark")

    cs = stock["close"]
    cm = market["close"]

    # Verification de la derniere barre disponible.
    required_end = m0 + post * MIN

    if cs.index.max() < required_end:
        raise IncompleteEventError(
            f"Fenetre incomplete : titre, derniere barre "
            f"{cs.index.max()}, fin attendue {required_end}"
        )

    if cm.index.max() < required_end:
        raise IncompleteEventError(
            f"Fenetre incomplete : benchmark, derniere barre "
            f"{cm.index.max()}, fin attendue {required_end}"
        )

    # Estimation strictement anterieure a la fenetre pre-evenement.
    est_end = m0 - (pre + 1) * MIN

    mm = estimate_market_model(
        log_returns(cs.loc[:est_end]),
        log_returns(cm.loc[:est_end]),
        est_minutes,
    )

    w = _window_returns(
        cs,
        cm,
        m0,
        pre,
        post,
    )

    w["ar"] = (
        w["r_stock"]
        - (
            mm["alpha"]
            + mm["beta"] * w["r_mkt"]
        )
    )

    # Exclusion de la minute contenant la publication.
    w.loc[w["k"] == 0, "ar"] = np.nan

    # Verification stricte des fenetres CAR.
    for label, lo, hi in (
        ("15 minutes", 1, 15),
        ("60 minutes", 1, 60),
    ):
        expected = hi - lo + 1

        selected = w.loc[
            w["k"].between(lo, hi),
            "ar",
        ]

        actual = int(selected.notna().sum())

        if actual != expected:
            raise IncompleteEventError(
                f"Fenetre incomplete ({label}) : "
                f"{actual}/{expected} rendements valides"
            )

    # Verification de la periode anterieure.
    pre_returns = w.loc[
        w["k"].between(-pre, -1),
        "ar",
    ]

    actual_pre = int(pre_returns.notna().sum())

    if actual_pre != pre:
        raise IncompleteEventError(
            f"Fenetre pre-evenement incomplete : "
            f"{actual_pre}/{pre} rendements valides"
        )

    def car(lo, hi):
        selected = w.loc[
            w["k"].between(lo, hi),
            "ar",
        ].dropna()

        c = float(selected.sum())
        n = len(selected)

        t = c / (mm["sigma"] * np.sqrt(n))

        p = float(
            2 * stats.t.sf(
                abs(t),
                mm["n_est"] - 2,
            )
        )

        return c, n, float(t), p

    pre_c, _, pre_t, pre_p = car(-pre, -1)
    c15, n15, t15, p15 = car(1, 15)
    c60, n60, t60, p60 = car(1, 60)

    vol = stock["volume"]

    v_est = (
        vol.loc[:est_end]
        .tail(est_minutes)
        .mean()
    )

    v_15 = vol.loc[
        m0 + MIN:m0 + 15 * MIN
    ].mean()

    w["car"] = (
        w["ar"]
        .where(w["k"] >= 1)
        .cumsum()
    )

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
        "rel_volume_15": (
            float(v_15 / v_est)
            if pd.notna(v_est)
            and pd.notna(v_15)
            and v_est > 0
            else np.nan
        ),
        "leak_flag": bool(pre_p < 0.05),
    }

    return summary, w.reset_index(names="ts_utc")


def cross_sectional_test(cars) -> dict:
    """Teste si le CAR moyen est different de zero."""
    s = pd.Series(cars, dtype=float).dropna()

    if len(s) < 2:
        return {
            "n": len(s),
            "mean_car": s.mean() if len(s) else np.nan,
            "t_stat": np.nan,
            "p_ttest": np.nan,
            "p_wilcoxon": np.nan,
        }

    t, p = stats.ttest_1samp(s, 0.0)

    wp = (
        stats.wilcoxon(s).pvalue
        if len(s) > 5 and (s != 0).any()
        else np.nan
    )

    return {
        "n": len(s),
        "mean_car": s.mean(),
        "t_stat": t,
        "p_ttest": p,
        "p_wilcoxon": wp,
    }
