"""Association article -> ticker avec score de confiance."""

from __future__ import annotations

import re


def build_alias_patterns(alias_map: dict[str, list[str]]) -> dict[str, re.Pattern]:
    """alias_map : {'AAPL': ['Apple', 'Apple Inc']} -> motifs compilés."""
    pats = {}
    for tkr, names in alias_map.items():
        alts = [rf"\${re.escape(tkr)}\b", rf"\b{re.escape(tkr)}\b"] + [rf"\b{re.escape(n)}\b" for n in names]
        pats[tkr] = re.compile("|".join(alts))
    return pats


def associate(headline: str, body: str, provided: list[str] | None, pats: dict[str, re.Pattern]) -> list[tuple[str, float, str]]:
    """Retourne [(ticker, confiance, méthode)]. Source > titre > corps."""
    out: dict[str, tuple[float, str]] = {t: (1.0, "source") for t in (provided or [])}
    for tkr, p in pats.items():
        if tkr in out:
            continue
        if p.search(headline or ""):
            out[tkr] = (0.9, "titre")
        elif p.search(body or ""):
            out[tkr] = (0.6, "corps")
    return [(t, c, m) for t, (c, m) in out.items()]
