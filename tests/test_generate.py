"""Tests for src/scripts/generate.py"""
import pandas as pd
import pytest

from src.scripts.generate import (
    Restaurant,
    _build_rationale,
    _make_entries,
    generate_script,
    save_script,
)


def _scored_row(**kwargs) -> dict:
    defaults = dict(
        district="마포구", neighborhood="도화동", category="일식",
        store_count=10, foot_traffic=0.8, card_payment=0.6,
        commercial_density=0.4, score=0.68,
    )
    return {**defaults, **kwargs}


def _make_ranked(n: int = 5) -> pd.DataFrame:
    rows = [_scored_row(store_count=10 - i, score=1.0 - i * 0.1) for i in range(n)]
    return pd.DataFrame(rows)


# ── _build_rationale ──────────────────────────────────────────────────────────

def test_build_rationale_contains_score():
    row = pd.Series(_scored_row(score=0.72))
    rationale = _build_rationale(row)
    assert "0.72" in rationale


def test_build_rationale_high_foot_traffic_label():
    row = pd.Series(_scored_row(foot_traffic=0.9))
    assert "always buzzing with locals" in _build_rationale(row)


def test_build_rationale_low_commercial_density_label():
    row = pd.Series(_scored_row(commercial_density=0.1))
    assert "smaller, more intimate dining scene" in _build_rationale(row)


# ── _make_entries ─────────────────────────────────────────────────────────────

def test_make_entries_uses_restaurant_names():
    ranked = _make_ranked(2)
    restaurants = [
        Restaurant(name="스시야", address="도화동 12-3"),
        Restaurant(name="오마카세", address="도화동 45"),
    ]
    entries = _make_entries(ranked, restaurants)
    assert "스시야" in entries
    assert "오마카세" in entries


def test_make_entries_falls_back_to_placeholder():
    ranked = _make_ranked(3)
    entries = _make_entries(ranked, restaurants=None)
    assert "[Restaurant 1 — fill in name]" in entries
    assert "[Restaurant 3 — fill in name]" in entries


def test_make_entries_partial_restaurants():
    ranked = _make_ranked(5)
    restaurants = [Restaurant(name="스시야", address="도화동 12-3")]
    entries = _make_entries(ranked, restaurants)
    assert "스시야" in entries
    assert "[Restaurant 2 — fill in name]" in entries


# ── generate_script ───────────────────────────────────────────────────────────

def test_generate_script_calls_rank_and_renders(monkeypatch):
    ranked = _make_ranked(5)
    monkeypatch.setattr("src.scripts.generate.rank", lambda df, d, n, c: ranked)

    df = pd.DataFrame([_scored_row()])
    script = generate_script("마포구", "도화동", "일식", df)

    assert "Top 5 일식 Spots in 도화동, 마포구, Seoul" in script
    assert "#1 —" in script
    assert "#5 —" in script


def test_generate_script_includes_data_attribution(monkeypatch):
    ranked = _make_ranked(1)
    monkeypatch.setattr("src.scripts.generate.rank", lambda df, d, n, c: ranked)
    df = pd.DataFrame([_scored_row()])
    script = generate_script("마포구", "도화동", "일식", df)
    assert "Seoul Open Data Portal" in script


# ── save_script ───────────────────────────────────────────────────────────────

def test_save_script_writes_file(tmp_path, monkeypatch):
    monkeypatch.setattr("src.scripts.generate.OUTPUTS_DIR", tmp_path)
    path = save_script("script content", "마포구", "도화동", "일식")
    assert path.exists()
    assert path.read_text(encoding="utf-8") == "script content"


def test_save_script_filename_format(tmp_path, monkeypatch):
    monkeypatch.setattr("src.scripts.generate.OUTPUTS_DIR", tmp_path)
    path = save_script("x", "마포구", "도화동", "일식")
    assert path.name == "마포구_도화동_일식.md"
