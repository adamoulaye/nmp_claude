"""Evalue FinBERT (modele 1) sur l'echantillon annote, contre un lexique et la classe majoritaire."""
import pathlib, re
import pandas as pd
from sklearn.metrics import accuracy_score, cohen_kappa_score, confusion_matrix, f1_score

D = pathlib.Path("data/exports")
L = ["negative", "neutral", "positive"]
# Meme lexique et meme regle que nmp.local.robustness.lexicon_scorer
POS = set("""beat beats beating surge surges surged soar soars soared jump jumps jumped rally rallies rallied rise rises rose gain gains
gained upgrade upgrades upgraded raises raised record strong tops topped outperform outperforms boost boosts growth approval approved
bullish overweight climb climbs win wins positive""".split())
NEG = set("""miss misses missed plunge plunges plunged tumble tumbles tumbled fall falls fell drop drops dropped slump slumps sink sinks
downgrade downgrades downgraded cut cuts lower lowers lowered lawsuit sues sued probe investigation recall recalls warns warning weak
worst retreat retreats slides slid loss losses decline declines bearish underweight fraud halt halts negative""".split())


def lex(t):
    toks = re.findall(r"[a-z']+", (t or "").lower())
    p, n = sum(w in POS for w in toks), sum(w in NEG for w in toks)
    return "positive" if p > n else "negative" if n > p else "neutral"


a = pd.read_csv(D / "annotations_3_modeles.csv", sep=";", encoding="utf-8-sig")
k = pd.read_csv(D / "sentiment_sample_cle.csv", sep=";", encoding="utf-8-sig")
m = a.merge(k[["id", "finbert_label"]], on="id")
assert len(m) == 107, "il faut 107 articles apres la jointure"
m["lexique"] = m["texte"].map(lex)
rows = []
for gold in ["consensus2sur3", "claude", "gpt", "copilot"]:
    for sub, mask in [("tous", m.index == m.index), ("lies_au_symbole", m.lie_au_symbole == "oui")]:
        d = m[mask]
        maj = d[gold].mode()[0]
        for name, pred in [("finbert", d.finbert_label), ("lexique", d.lexique), ("toujours_" + maj, pd.Series(maj, index=d.index))]:
            rows.append(dict(reference=gold, echantillon=sub, n=len(d), modele=name,
                             f1_macro=f1_score(d[gold], pred, labels=L, average="macro", zero_division=0),
                             accuracy=accuracy_score(d[gold], pred),
                             kappa=cohen_kappa_score(d[gold], pred, labels=L)))
r = pd.DataFrame(rows).round(3)
r.to_csv(D / "sentiment_eval_resultats.csv", sep=";", index=False, encoding="utf-8-sig")
pd.set_option("display.width", 160)
print(r.to_string(index=False))
print("\nMatrice de confusion consensus 2/3 (lignes) contre FinBERT (colonnes) :")
print(pd.DataFrame(confusion_matrix(m.consensus2sur3, m.finbert_label, labels=L), index=L, columns=L))
print("\nRepartition FinBERT :", m.finbert_label.value_counts().to_dict(), "| consensus :", m.consensus2sur3.value_counts().to_dict())
