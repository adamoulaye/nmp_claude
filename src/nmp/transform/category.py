"""Categorie d'evenement: ligne de base a base de regles (rapide, explicable).
A comparer ensuite a un modele zero-shot ou fine-tune sur votre echantillon annote.
Un mot-cle correspond en DEBUT de mot (evite 'eps' dans 'steps'); un espace final = mot entier."""

import re

from nmp.ml.sentiment import ITEM_8K

RULES = [  # l'ordre compte: la premiere regle qui correspond gagne
    ("résultats", ["earnings", "eps ", "revenue", "quarterly", "guidance", "profit", "q1", "q2", "q3", "q4"]),
    ("fusion-acquisition", ["acquire", "acquisition", "merger", "buyout", "takeover", "to buy "]),
    (
        "réglementaire-juridique",
        ["lawsuit", "sues", "sued", "settlement", "court", "class action", "verdict"],
    ),
    (
        "réglementaire-juridique",
        ["sec ", "antitrust", "regulator", "probe", "investigation", "ftc", "doj", "fine", "approval", "fda"],
    ),
    ("direction", ["ceo", "cfo", "resign", "steps down", "appoints", "chairman", "names new"]),
    ("analyste-notation", ["upgrade", "downgrade", "price target", "analyst", "rating", "initiates"]),
    ("autre", ["dividend", "buyback", "repurchase"]),
    ("produit", ["launch", "unveil", "release", "introduces", "new model", "partnership"]),
]
SEC_8K_ITEMS = ITEM_8K  # une seule table items 8-K -> catégorie


def _compile(words):
    parts = [re.escape(w.strip()) + (r"\b" if w.endswith(" ") else "") for w in words]
    return re.compile(r"\b(?:" + "|".join(parts) + ")")


_COMPILED = [(label, _compile(words)) for label, words in RULES]


def categorize(headline: str, summary: str = "") -> str:
    text = f"{headline} {summary}".lower()
    for label, pattern in _COMPILED:
        if pattern.search(text):
            return label
    return "autre"


def categorize_8k(items: str) -> str:
    """items : chaîne EDGAR du type '2.02,9.01'. Retourne la catégorie du premier item connu (hors 'autre')."""
    for it in (items or "").split(","):
        if SEC_8K_ITEMS.get(it.strip(), "autre") != "autre":
            return SEC_8K_ITEMS[it.strip()]
    return "autre"
