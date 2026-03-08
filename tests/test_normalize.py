"""Tests for src/pipeline/normalize.py"""
import pandas as pd
import pytest

from src.pipeline.normalize import (
    minmax,
    normalize_card_payment,
    normalize_commercial_district,
    normalize_foot_traffic,
)


# ── normalize_foot_traffic ────────────────────────────────────────────────────

def test_foot_traffic_sums_rows():
    rows = [
        {"ADSTRD_CODE_SE": "11440680", "TOT_LVPOP_CO": "3000"},
        {"ADSTRD_CODE_SE": "11440680", "TOT_LVPOP_CO": "2000"},
    ]
    df = normalize_foot_traffic(rows)
    assert df.iloc[0]["foot_traffic"] == 5000.0


def test_foot_traffic_empty_returns_empty():
    df = normalize_foot_traffic([])
    assert df.empty


def test_foot_traffic_invalid_values_treated_as_zero():
    rows = [{"ADSTRD_CODE_SE": "11440680", "TOT_LVPOP_CO": "N/A"}]
    df = normalize_foot_traffic(rows)
    assert df.iloc[0]["foot_traffic"] == 0.0


# ── normalize_card_payment ────────────────────────────────────────────────────

def test_card_payment_sums_rows():
    rows = [
        {"GU_NM": "마포구", "DONG_NM": "도화동", "THSMON_SELNG_AMT": "5000000"},
        {"GU_NM": "마포구", "DONG_NM": "도화동", "THSMON_SELNG_AMT": "3000000"},
    ]
    df = normalize_card_payment(rows)
    assert df.iloc[0]["card_payment"] == 8_000_000.0


def test_card_payment_empty_returns_empty():
    df = normalize_card_payment([])
    assert df.empty


# ── normalize_commercial_district ─────────────────────────────────────────────

def test_commercial_district_computes_density():
    rows = [
        {
            "GU_NM": "마포구", "DONG_NM": "도화동",
            "SVC_INDUTY_NM": "일식",
            "STOR_CO": "10", "OPBIZ_RT": "0.2", "CLSBIZ_RT": "0.1",
        }
    ]
    df = normalize_commercial_district(rows)
    # density = 10 * (1 + 0.2 - 0.1) = 11.0
    assert df.iloc[0]["commercial_density"] == pytest.approx(11.0)


def test_commercial_district_groups_by_category():
    rows = [
        {"GU_NM": "마포구", "DONG_NM": "도화동", "SVC_INDUTY_NM": "일식",
         "STOR_CO": "5", "OPBIZ_RT": "0.1", "CLSBIZ_RT": "0.05"},
        {"GU_NM": "마포구", "DONG_NM": "도화동", "SVC_INDUTY_NM": "한식",
         "STOR_CO": "20", "OPBIZ_RT": "0.3", "CLSBIZ_RT": "0.1"},
    ]
    df = normalize_commercial_district(rows)
    assert set(df["category"]) == {"일식", "한식"}


def test_commercial_district_empty_returns_empty():
    df = normalize_commercial_district([])
    assert df.empty


# ── minmax ────────────────────────────────────────────────────────────────────

def test_minmax_scales_to_0_1():
    s = pd.Series([0.0, 50.0, 100.0])
    result = minmax(s)
    assert result.tolist() == pytest.approx([0.0, 0.5, 1.0])


def test_minmax_constant_series_returns_zeros():
    s = pd.Series([7.0, 7.0, 7.0])
    result = minmax(s)
    assert result.tolist() == [0.0, 0.0, 0.0]
