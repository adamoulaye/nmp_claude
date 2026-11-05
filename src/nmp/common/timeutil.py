"""Temps : UTC partout, calendrier boursier, rattachement des nouvelles à une séance.

Règle anti-biais d'anticipation : une nouvelle n'est exploitable qu'à `published_utc`
(jamais à sa mise à jour ultérieure) et la réaction est mesurée à `reaction_start_utc`.
"""

from __future__ import annotations

from functools import lru_cache

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

ET = "America/New_York"


@lru_cache(maxsize=4)
def _calendar(name: str):
    return mcal.get_calendar(name)


def to_utc(series: pd.Series, *, naive_tz: str | None = None) -> pd.Series:
    """Convertit en UTC. `naive_tz` : fuseau supposé pour les horodatages sans fuseau (ex. ET)."""
    if naive_tz is None:
        return pd.to_datetime(series, errors="coerce", utc=True)
    s = pd.to_datetime(series, errors="coerce")
    return s.dt.tz_localize(naive_tz, ambiguous="NaT", nonexistent="shift_forward").dt.tz_convert("UTC")


def build_sessions(start: str, end: str, calendar: str = "NYSE") -> pd.DataFrame:
    """Séances avec ouverture/fermeture en UTC (DST, jours fériés, fermetures anticipées)."""
    sched = _calendar(calendar).schedule(start_date=start, end_date=end)
    return pd.DataFrame(
        {
            "session_date": sched.index.date,
            "open_utc": pd.DatetimeIndex(sched["market_open"]).tz_convert("UTC"),
            "close_utc": pd.DatetimeIndex(sched["market_close"]).tz_convert("UTC"),
        }
    )


def _ns(s) -> np.ndarray:
    idx = pd.DatetimeIndex(s)
    idx = idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")
    return idx.tz_localize(None).to_numpy().astype("datetime64[ns]")


def attach_session(df: pd.DataFrame, sessions: pd.DataFrame, ts_col: str = "published_utc") -> pd.DataFrame:
    """Ajoute session_type (pendant/avant/apres/ferme), session_date, reaction_start_utc.

    - pendant : réaction mesurée dès la publication
    - avant / apres / ferme (week-end, férié) : réaction mesurée à l'ouverture suivante
    """
    out = df.copy()
    t = pd.to_datetime(out[ts_col], utc=True)
    tn, opens, closes = _ns(t), _ns(sessions["open_utc"]), _ns(sessions["close_utc"])
    sd = sessions["session_date"].to_numpy()
    n = len(sessions)
    idx = np.searchsorted(opens, tn, side="right") - 1
    prev, nxt = np.clip(idx, 0, n - 1), np.clip(idx + 1, 0, n - 1)
    et_date = t.dt.tz_convert(ET).dt.date.to_numpy()

    during = (idx >= 0) & (tn < closes[prev])
    pre_mkt = (idx + 1 < n) & (et_date == sd[nxt]) & ~during
    after = (idx >= 0) & (et_date == sd[prev]) & ~during
    out["session_type"] = np.select([during, pre_mkt, after], ["pendant", "avant", "apres"], default="ferme")

    react = pd.Series(pd.to_datetime(np.where(during, tn, opens[nxt]), utc=True), index=out.index)
    # calendrier trop court -> NaT (sauf 'pendant' : réaction immédiate)
    out["reaction_start_utc"] = react.mask((idx + 1 >= n) & ~during)
    out["session_date"] = np.where(during, sd[prev], sd[nxt])
    return out


def classify_session(ts, calendar: str = "NYSE") -> tuple[str, pd.Timestamp]:
    """Version « un seul instant » de attach_session : (session_type, reaction_start_utc).

    session_type : 'pendant' | 'avant' | 'apres' | 'ferme' (week-end, férié). Pratique pour les scripts et les
    alertes ; pour des milliers d'articles, utiliser build_sessions + attach_session (vectorisé).
    """
    t = pd.Timestamp(ts)
    t = t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")
    sess = build_sessions((t - pd.Timedelta(days=7)).date().isoformat(), (t + pd.Timedelta(days=7)).date().isoformat(), calendar)
    row = attach_session(pd.DataFrame({"published_utc": [t]}), sess).iloc[0]
    return row["session_type"], row["reaction_start_utc"]
