"""Importance par permutation du champion sur le holdout publie (271 evenements)."""
import json, os, subprocess
import numpy as np
import pandas as pd
os.environ["DATABRICKS_CONFIG_PROFILE"] = "nmp"
import mlflow
from sklearn.inspection import permutation_importance
from sklearn.metrics import f1_score
from nmp.ml.event_classifier import CAT_FEATURES, NUM_FEATURES, make_target, prepare

MODEL = "event_magnitude_random_forest"

def sql(stmt):
    body = json.dumps({"warehouse_id": "601d8e918b421d97", "catalog": "mnp_dev", "wait_timeout": "50s", "statement": stmt})
    p = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", "nmp", "--json", body], capture_output=True, text=True)
    r = json.loads(p.stdout)
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(r["status"].get("error", {}).get("message", "")[:300])
    cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
    return pd.DataFrame(r["result"].get("data_array", []), columns=cols)

df = sql("SELECT * FROM mnp_dev.gold.v_training_set")
pred = sql(f"SELECT hk_article, hk_security FROM mnp_dev.gold.model_predictions WHERE model_name = '{MODEL}'")
for c in ["sentiment_score", "n_tickers", "pre_vol_15m", "abs_car"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df["is_first"] = df["is_first"].astype(str).str.lower().eq("true")
df["published_utc"] = pd.to_datetime(df["published_utc"], utc=True)
d = prepare(df.sort_values("published_utc").reset_index(drop=True))
d = d.replace([np.inf, -np.inf], np.nan).dropna(subset=["published_utc", "abs_car"] + CAT_FEATURES + NUM_FEATURES)
key = set(zip(pred["hk_article"], pred["hk_security"]))
is_test = pd.Series([k in key for k in zip(d["hk_article"], d["hk_security"])], index=d.index)
t0 = d.loc[is_test, "published_utc"].min()
train_mask = d["published_utc"] < t0
d["magnitude"] = make_target(d["abs_car"], train_mask)
te = d[is_test]
print("holdout =", len(te), "| entrainement reconstitue =", int(train_mask.sum()), "| predictions publiees =", len(pred))
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
m = mlflow.sklearn.load_model(f"models:/mnp_dev.gold.{MODEL}/3")
X, y = te[CAT_FEATURES + NUM_FEATURES], te["magnitude"]
f1 = f1_score(y, m.predict(X), average="macro", zero_division=0)
print("F1 macro recalcule =", round(f1, 4), "(publie : 0.4102)")
pi = permutation_importance(m, X, y, scoring="f1_macro", n_repeats=30, random_state=42, n_jobs=1)
out = pd.DataFrame({"feature": X.columns, "importance_mean": pi.importances_mean, "importance_std": pi.importances_std})
out = out.sort_values("importance_mean", ascending=False).reset_index(drop=True)
out["model_name"] = MODEL
out["model_version"] = "3"
out["n_test"] = len(te)
out["scoring"] = "f1_macro"
out.to_csv("feature_importance.csv", index=False)
print(out[["feature", "importance_mean", "importance_std"]].round(4).to_string(index=False))
