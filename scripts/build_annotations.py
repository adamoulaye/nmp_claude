"""Reconstruit annotations_3_modeles.csv (texte + 3 annotateurs + consensus) a partir des etiquettes seules."""
import pathlib
import pandas as pd

D = pathlib.Path("data/exports")
t = pd.read_csv(D / "sentiment_sample_a_annoter.csv", sep=";", encoding="utf-8-sig")[["id", "date", "symbole", "source", "texte"]]
lab = pd.read_csv(D / "labels_3_annotateurs.csv", sep=";")
m = t.merge(lab, on="id")
assert len(m) == 107, "attendu : 107 articles"
cols = ["claude", "gpt", "copilot"]
m["consensus2sur3"] = m[cols].apply(lambda r: r.mode().iloc[0] if r.value_counts().iloc[0] >= 2 else None, axis=1)
m.to_csv(D / "annotations_3_modeles.csv", sep=";", index=False, encoding="utf-8-sig")
print("ecrit :", D / "annotations_3_modeles.csv", len(m), "articles ; consensus :", m.consensus2sur3.value_counts().to_dict())
