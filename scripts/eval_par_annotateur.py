"""FinBERT contre chaque annotateur (Claude, GPT, Copilot) : matrice de confusion + P/R/F1 par classe."""
from pathlib import Path
import pandas as pd
from sklearn.metrics import confusion_matrix, precision_recall_fscore_support

D = Path(__file__).resolve().parents[1] / "data" / "exports"
L = ["negative", "neutral", "positive"]
a = pd.read_csv(D / "annotations_3_modeles.csv", sep=";", encoding="utf-8-sig")
k = pd.read_csv(D / "sentiment_sample_cle.csv", sep=";", encoding="utf-8-sig")
m = a.merge(k[["id", "finbert_label"]], on="id")
assert len(m) == 107
rows = []
for who in ["claude", "gpt", "copilot"]:
    print(f"\n=== {who} (lignes) contre FinBERT (colonnes) ===")
    print(pd.DataFrame(confusion_matrix(m[who], m.finbert_label, labels=L), index=L, columns=L))
    p, r, f, s = precision_recall_fscore_support(m[who], m.finbert_label, labels=L, zero_division=0)
    for i, c in enumerate(L):
        rows.append(dict(annotateur=who, classe=c, support=int(s[i]), precision_finbert=p[i], rappel_finbert=r[i], f1=f[i]))
r = pd.DataFrame(rows).round(3)
print("\n", r.to_string(index=False))
r.to_csv(D / "sentiment_eval_par_annotateur.csv", sep=";", index=False, encoding="utf-8-sig")
