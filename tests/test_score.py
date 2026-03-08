"""Tests for src/scoring/score.py"""
import pandas as pd
import pytest

from src.scoring.score import compute_scores, rank, top5


def _make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


BASE_ROW = dict(
    district="마포구",
    neighborhood="도화동",
    store_count=10,
    foot_traffic=0.5,
    card_payment=0.5,
    commercial_density=0.5,
)


# ── compute_scores ────────────────────────────────────────────────────────────

def test_compute_scores_adds_score_column():
    df = _make_df([{**BASE_ROW, "category": "일식"}])
    result = compute_scores(df)
    assert "score" in result.columns


def test_compute_scores_sorts_descending():
    df = _make_df([
        {**BASE_ROW, "category": "일식", "foot_traffic": 0.2, "card_payment": 0.2, "commercial_density": 0.2},
        {**BASE_ROW, "category": "한식", "foot_traffic": 0.9, "card_payment": 0.9, "commercial_density": 0.9},
    ])
    result = compute_scores(df)
    assert result.iloc[0]["category"] == "한식"


def test_compute_scores_correct_value():
    df = _make_df([{**BASE_ROW, "category": "일식",
                    "foot_traffic": 1.0, "card_payment": 1.0, "commercial_density": 1.0}])
    result = compute_scores(df)
    # 1.0*0.4 + 1.0*0.4 + 1.0*0.2 = 1.0
    assert result.iloc[0]["score"] == pytest.approx(1.0)


# ── top5 ──────────────────────────────────────────────────────────────────────

def test_top5_returns_at_most_5():
    rows = [
        {**BASE_ROW, "category": "일식", "foot_traffic": i / 10}
        for i in range(10)
    ]
    df = _make_df(rows)
    result = top5("마포구", "도화동", "일식", df)
    assert len(result) <= 5


def test_top5_filters_by_district_and_neighborhood():
    rows = [
        {**BASE_ROW, "category": "일식"},
        {**BASE_ROW, "district": "용산구", "neighborhood": "이태원동", "category": "일식"},
    ]
    df = _make_df(rows)
    result = top5("마포구", "도화동", "일식", df)
    assert all(result["district"] == "마포구")
    assert all(result["neighborhood"] == "도화동")


def test_top5_filters_by_category():
    rows = [
        {**BASE_ROW, "category": "일식"},
        {**BASE_ROW, "category": "한식"},
    ]
    df = _make_df(rows)
    result = top5("마포구", "도화동", "한식", df)
    assert all(result["category"] == "한식")


# ── rank ──────────────────────────────────────────────────────────────────────

def test_rank_raises_with_available_categories():
    df = _make_df([{**BASE_ROW, "category": "한식"}])
    with pytest.raises(ValueError, match="한식"):
        rank(df, "마포구", "도화동", "일식")


def test_rank_returns_scored_df():
    df = _make_df([{**BASE_ROW, "category": "일식"}])
    result = rank(df, "마포구", "도화동", "일식")
    assert "score" in result.columns
    assert len(result) == 1
