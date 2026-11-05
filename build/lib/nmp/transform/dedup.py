"""Regroupement des articles quasi identiques : 1re publication = signal, reprises = bruit.

Deux règles se combinent (aucune ligne n'est supprimée, la provenance est conservée) :
  1. texte : MinHash-LSH sur des shingles de k mots (k=3 pour titre+résumé ; k=1 pour des titres seuls) ;
  2. URL canonique identique (optionnel, `url_col`) -> même groupe, même si le texte diffère.
Une reprise hors fenêtre (48 h par défaut) est une nouvelle information.
"""

from __future__ import annotations

import re
from urllib.parse import urlsplit

import numpy as np
import pandas as pd


def canonical_url(url: str | None) -> str:
    """Domaine + chemin, sans www, paramètres, ancre ni schéma (utm_*, http/https...)."""
    if not url or not isinstance(url, str):
        return ""
    p = urlsplit(url.strip())
    return f"{p.netloc.lower().removeprefix('www.')}{p.path.rstrip('/')}"


def _shingles(text: str | None, k: int = 3) -> set[str]:
    toks = re.findall(r"\w+", (text or "").lower() if isinstance(text, str) else "")
    if not toks:
        return set()  # texte vide : rien à comparer (ne JAMAIS regrouper des articles sans texte)
    if len(toks) <= k:
        return {" ".join(toks)}
    return {" ".join(toks[i : i + k]) for i in range(len(toks) - k + 1)}


def cluster_articles(
    df: pd.DataFrame,
    *,
    id_col="hk_article",
    text_col="text",
    ts_col="published_utc",
    url_col: str | None = None,
    threshold: float = 0.8,
    window: str = "48h",
    num_perm: int = 128,
    k: int = 3,
) -> pd.DataFrame:
    """Ajoute cluster_id, is_first, dup_rank, cluster_size (parcours chronologique).

    À l'échelle : appeler via groupBy(date).applyInPandas sur une fenêtre glissante de 48 h.
    """
    from datasketch import MinHash, MinHashLSH

    d = df.sort_values(ts_col, kind="stable").reset_index(drop=True)
    ts = pd.to_datetime(d[ts_col], utc=True).to_numpy()
    win = pd.Timedelta(window).to_timedelta64()
    urls = d[url_col].tolist() if url_col else [None] * len(d)
    lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
    url_last: dict[str, int] = {}
    cluster = np.empty(len(d), dtype=object)
    for i, text in enumerate(d[text_col].tolist()):
        shingles, cands = _shingles(text, k), []
        if shingles:
            m = MinHash(num_perm=num_perm)
            for sh in shingles:
                m.update(sh.encode())
            cands = [int(c) for c in lsh.query(m) if ts[i] - ts[int(c)] <= win]
        cu = canonical_url(urls[i]) if url_col else ""
        if cu and cu in url_last and ts[i] - ts[url_last[cu]] <= win:
            cands.append(url_last[cu])
        cluster[i] = cluster[min(cands)] if cands else d.at[i, id_col]
        if shingles:
            lsh.insert(str(i), m)
        if cu:
            url_last[cu] = i
    d["cluster_id"] = cluster
    d["dup_rank"] = d.groupby("cluster_id").cumcount()
    d["is_first"] = d["dup_rank"] == 0
    d["cluster_size"] = d.groupby("cluster_id")[id_col].transform("size")
    return d
