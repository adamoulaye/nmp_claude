"""Tests du scorer lexical et de add_sentiment (local/robustness.py) : fonctions pures, sans Spark ni modèle."""

import pandas as pd
import pytest

from nmp.local.robustness import add_sentiment, lexicon_scorer

COLS = ["positive", "negative", "neutral", "sentiment_score", "sentiment_label"]


def test_lexicon_positive():
    r = lexicon_scorer(["Stock surges after record beat"]).iloc[0]
    assert r["sentiment_label"] == "positive"
    assert r["sentiment_score"] == pytest.approx(0.75)
    assert r["neutral"] == pytest.approx(0.25)


def test_lexicon_negative():
    r = lexicon_scorer(["Company warns of lawsuit and plunge"]).iloc[0]
    assert r["sentiment_label"] == "negative"
    assert r["sentiment_score"] == pytest.approx(-0.75)


def test_lexicon_neutral_and_tie():
    df = lexicon_scorer(["Company holds annual meeting", "beat miss", ""])
    assert df["sentiment_label"].tolist() == ["neutral"] * 3
    assert df["sentiment_score"].tolist() == [0.0, 0.0, 0.0]
    assert df.loc[0, "neutral"] == pytest.approx(1.0)


def test_lexicon_columns_and_empty():
    assert list(lexicon_scorer(["beat"]).columns) == COLS
    assert lexicon_scorer([]).empty


def test_add_sentiment_aucun_and_empty_return_input():
    res = pd.DataFrame({"headline": ["beat"]})
    assert add_sentiment(res, method="aucun") is res
    vide = pd.DataFrame(columns=["headline"])
    assert add_sentiment(vide) is vide


def test_add_sentiment_lexique_adds_columns_without_mutating():
    res = pd.DataFrame({"headline": ["Shares surge on record growth", None, "Firm faces fraud probe"]})
    out = add_sentiment(res)
    assert "sentiment_label" not in res.columns
    assert out["sentiment_label"].tolist() == ["positive", "neutral", "negative"]
    assert set(out["sentiment_methode"]) == {"lexique"}


def test_add_sentiment_custom_scorer():
    def fake(texts):
        return pd.DataFrame({c: [0.5] * len(texts) for c in COLS[:4]} | {"sentiment_label": ["neutral"] * len(texts)})

    out = add_sentiment(pd.DataFrame({"headline": ["a", "b"]}), method="finbert", scorer=fake)
    assert out["sentiment_score"].tolist() == [0.5, 0.5]
    assert set(out["sentiment_methode"]) == {"finbert"}
