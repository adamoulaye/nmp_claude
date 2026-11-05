import pandas as pd
import pytest

pytest.importorskip("datasketch")
from nmp.transform.dedup import cluster_articles  # noqa: E402


def test_first_publication_is_signal():
    base = "Apple reports record quarterly revenue driven by strong iPhone sales and services growth across all regions"
    df = pd.DataFrame(
        {
            "hk_article": ["a", "b", "c", "d"],
            "text": [
                base,
                base + " says Reuters",
                "Tesla recalls vehicles over software issue in new regulatory filing today",
                base,
            ],
            "published_utc": pd.to_datetime(
                ["2024-05-02 20:00", "2024-05-02 20:05", "2024-05-02 20:10", "2024-05-02 21:00"], utc=True
            ),
        }
    )
    out = cluster_articles(df, threshold=0.6).set_index("hk_article")
    assert out.loc["a", "is_first"] and not out.loc["b", "is_first"] and not out.loc["d", "is_first"]
    assert out.loc["c", "is_first"]
    assert out.loc[["a", "b", "d"], "cluster_id"].nunique() == 1 and out.loc["a", "cluster_size"] == 3


def test_reprise_hors_fenetre_est_nouveau_signal():
    t = "Fed holds interest rates steady and signals patience on future cuts amid inflation data"
    df = pd.DataFrame(
        {
            "hk_article": ["x", "y"],
            "text": [t, t],
            "published_utc": pd.to_datetime(["2024-05-01", "2024-05-10"], utc=True),
        }
    )
    assert cluster_articles(df).is_first.all()
