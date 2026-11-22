"""Tire un echantillon d'articles a annoter (modele 1) : un fichier sans score, un fichier cle."""
import csv, json, pathlib, subprocess, time

P, W, N = "nmp", "601d8e918b421d97", 17
OUT = pathlib.Path("data/exports")
SQL = f"""
WITH art AS (
  SELECT hk_article, published_utc, sentiment_score, sentiment_label, category, model_version
  FROM mnp_dev.business_vault.bv_article_enriched
  WHERE coalesce(is_first, true)
  QUALIFY row_number() OVER (PARTITION BY hk_article ORDER BY load_ts DESC) = 1
), txt AS (
  SELECT hk_article, headline, summary, source_name
  FROM mnp_dev.vault.sat_article_content
  QUALIFY row_number() OVER (PARTITION BY hk_article ORDER BY load_ts DESC) = 1
), sec AS (
  SELECT l.hk_article, min(d.symbol) AS symbol, min_by(d.sector, d.symbol) AS sector
  FROM mnp_dev.vault.link_article_security l JOIN mnp_dev.gold.dim_security d ON l.hk_security = d.hk_security
  GROUP BY l.hk_article
), j AS (
  SELECT a.*, t.headline, t.summary, t.source_name, s.symbol, s.sector,
         row_number() OVER (PARTITION BY s.sector ORDER BY xxhash64(a.hk_article)) AS rn
  FROM art a JOIN txt t ON a.hk_article = t.hk_article JOIN sec s ON a.hk_article = s.hk_article
  WHERE t.headline IS NOT NULL AND length(trim(t.headline)) > 15 AND t.headline NOT LIKE 'DEMO-%'
)
SELECT hk_article, published_utc, symbol, sector, source_name, headline, summary,
       sentiment_score, sentiment_label, category, model_version
FROM j WHERE rn <= {N} ORDER BY xxhash64(hk_article)
"""


def api(m, path, body=None):
    cmd = ["databricks", "api", m, path, "-p", P] + (["--json", json.dumps(body)] if body is not None else [])
    return json.loads(subprocess.run(cmd, capture_output=True, text=True).stdout)


def run(sql):
    d = api("post", "/api/2.0/sql/statements", {"warehouse_id": W, "statement": sql, "wait_timeout": "50s"})
    while d.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(5)
        d = api("get", "/api/2.0/sql/statements/" + d["statement_id"])
    if d.get("status", {}).get("state") != "SUCCEEDED":
        raise SystemExit(str(d.get("status", d)))
    cols = [c["name"] for c in d["manifest"]["schema"]["columns"]]
    return [dict(zip(cols, r)) for r in d.get("result", {}).get("data_array", [])]


def write(rows):
    OUT.mkdir(parents=True, exist_ok=True)
    a, k = OUT / "sentiment_sample_a_annoter.csv", OUT / "sentiment_sample_cle.csv"
    with open(a, "w", newline="", encoding="utf-8-sig") as fa, open(k, "w", newline="", encoding="utf-8-sig") as fk:
        wa, wk = csv.writer(fa, delimiter=";"), csv.writer(fk, delimiter=";")
        wa.writerow(["id", "date", "symbole", "source", "texte", "etiquette"])
        wk.writerow(["id", "hk_article", "secteur", "finbert_label", "finbert_score", "categorie", "model_version"])
        for i, r in enumerate(rows, 1):
            txt = ((r["headline"] or "").strip() + ". " + (r["summary"] or "").strip())[:1000]
            wa.writerow([i, (r["published_utc"] or "")[:10], r["symbol"], r["source_name"], txt, ""])
            wk.writerow([i, r["hk_article"], r["sector"], r["sentiment_label"], r["sentiment_score"], r["category"], r["model_version"]])
    return a, k, len(rows)


if __name__ == "__main__":
    print("fichiers : %s ; %s ; %d articles" % write(run(SQL)))
