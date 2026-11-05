"""Un test par défaut corrigé lors de la fusion + cas de séance repris de l'autre dépôt + lien CAR -> bv_event_car."""

import sys

import pandas as pd
import pytest

sys.path.insert(0, "tests")

from nmp.common.timeutil import attach_session, build_sessions, classify_session
from nmp.transform.adjust import adjust_intraday, adjustment_factor
from nmp.transform.car_batch import run_batch, to_bv_event_car
from nmp.transform.dedup import canonical_url, cluster_articles


def u(s):
    return pd.Timestamp(s, tz="UTC")


# ---- séances : cas de l'autre dépôt (fin de l'heure d'été, fermeture anticipée, Action de grâce) ----
@pytest.mark.parametrize(
    "ts, label, reaction",
    [
        ("2026-10-13 14:00", "pendant", "2026-10-13 14:00"),
        ("2026-10-13 22:00", "apres", "2026-10-14 13:30"),
        ("2026-10-14 11:00", "avant", "2026-10-14 13:30"),
        ("2026-10-17 15:00", "ferme", "2026-10-19 13:30"),  # samedi
        (
            "2026-11-02 14:29",
            "avant",
            "2026-11-02 14:30",
        ),  # après la fin de l'heure d'été : ouverture 14:30 UTC
        ("2026-11-02 14:30", "pendant", "2026-11-02 14:30"),
        ("2026-11-26 15:00", "ferme", "2026-11-27 14:30"),  # Action de grâce
        ("2026-11-27 19:00", "apres", "2026-11-30 14:30"),  # fermeture anticipée 13 h HNE
    ],
)
def test_classify_session(ts, label, reaction):
    got_label, got_reaction = classify_session(u(ts))
    assert got_label == label and got_reaction == u(reaction)


# ---- défaut 1 : des articles sans texte ne sont JAMAIS des doublons les uns des autres ----
def test_articles_sans_texte_ne_sont_pas_regroupes():
    df = pd.DataFrame(
        {
            "hk_article": ["a", "b", "c"],
            "text": [None, "", float("nan")],
            "published_utc": pd.to_datetime(["2024-05-02 10:00", "2024-05-02 11:00", "2024-05-02 12:00"], utc=True),
        }
    )
    out = cluster_articles(df)
    assert out["is_first"].all() and out["cluster_size"].eq(1).all()


# ---- défaut 2 : nouvelle publiée pendant la DERNIÈRE séance du calendrier ----
def test_derniere_seance_garde_sa_reaction():
    S = build_sessions("2024-05-01", "2024-05-03")
    r = attach_session(pd.DataFrame({"published_utc": [u("2024-05-03 15:00")]}), S).iloc[0]
    assert r.session_type == "pendant" and r.reaction_start_utc == u("2024-05-03 15:00")


# ---- défaut 3 : volume (ajusté par les splits seulement, jamais par les dividendes) ----
def test_volume_ajuste_par_split_pas_par_dividende():
    days = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]).date
    close = pd.Series([100.0, 100.0, 50.0, 50.0], index=days)
    f_all = adjustment_factor(close, splits=pd.Series({days[2]: 2.0}), dividends=pd.Series({days[3]: 1.0}))
    f_split = adjustment_factor(close, splits=pd.Series({days[2]: 2.0}))
    bars = pd.DataFrame({"bar_ts": pd.to_datetime(["2024-01-03 15:00"], utc=True), "close": [100.0], "volume": [1000.0]})
    out = adjust_intraday(bars, f_all, split_factor=f_split)
    assert out["volume"].iloc[0] == pytest.approx(1000.0 / 0.5)  # split 2:1 : volume x2
    assert adjust_intraday(bars, f_all)["volume"].iloc[0] == 1000.0  # sans split_factor : volume brut


# ---- règle URL (autre dépôt) et titres courts (k=1) ----
def test_canonical_url_ignore_query_et_www():
    assert canonical_url("https://www.Site.com/a/b/?utm=1#x") == canonical_url("http://site.com/a/b")


def test_meme_url_titres_differents_meme_groupe_et_fenetre():
    t = u("2026-10-13 14:00")
    df = pd.DataFrame(
        {
            "hk_article": ["a1", "a2", "a3", "a4"],
            "text": [
                "Apple beats earnings estimates",
                "AAPL tops forecasts says Reuters",
                "Apple beats earnings estimates",
                "Tesla recalls vehicles",
            ],
            "published_utc": [t, t + pd.Timedelta(minutes=10), t + pd.Timedelta(hours=60), t],
            "url": ["https://x.com/1?utm=1", "http://www.x.com/1", "https://z.com/3", "https://t.com/9"],
        }
    )
    out = cluster_articles(df, url_col="url", k=1).set_index("hk_article")
    assert out.loc["a1", "is_first"] and not out.loc["a2", "is_first"] and out.loc["a2", "cluster_id"] == "a1"
    assert out.loc["a3", "is_first"]  # 60 h plus tard : hors fenêtre de 48 h -> nouvelle information
    assert out.loc["a4", "is_first"] and len(out) == 4


# ---- le maillon manquant : événements -> lignes bv_event_car ----
def test_to_bv_event_car_une_ligne_par_fenetre():
    from test_event_study import make_bars

    s, m = make_bars(shock=0.004)
    bars = pd.concat([s.assign(ticker="AAPL"), m.assign(ticker="SPY")], ignore_index=True)
    ev = pd.DataFrame(
        {
            "hk_article": ["h1"],
            "hk_security": ["sec1"],
            "ticker": ["AAPL"],
            "published_utc": [u("2026-10-13 15:30:20")],
        }
    )
    res, _, err = run_batch(ev, bars, id_col="hk_article", keep=("hk_security",))
    out = to_bv_event_car(res)
    assert err.empty and sorted(out["window_end_min"]) == [15, 60]
    assert not out.duplicated(["hk_article", "hk_security", "window_end_min", "model_version"]).any()
    assert out.loc[out.window_end_min == 15, "car"].iloc[0] > 0.003 and set(out["window_start_min"]) == {1}
    assert {"alpha", "beta", "n_est", "leak_flag", "car_pre", "t_stat", "p_value", "load_ts"} <= set(out.columns)
