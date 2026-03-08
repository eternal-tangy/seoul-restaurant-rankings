"""Tests for src/pipeline/fetch.py — uses mocked HTTP responses."""
import json
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.fetch import (
    SVC_FOOT_TRAFFIC,
    _fetch_all,
    _get,
    fetch_card_payment,
    fetch_commercial_district,
    fetch_foot_traffic,
)


def _make_response(service: str, rows: list[dict], total: int | None = None) -> MagicMock:
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {
        service: {
            "list_total_count": total if total is not None else len(rows),
            "result": {"code": "INFO-000", "message": "정상 처리되었습니다"},
            "row": rows,
        }
    }
    return mock


# ── _get ──────────────────────────────────────────────────────────────────────

def test_get_returns_service_body():
    rows = [{"GU_NM": "마포구", "DONG_NM": "도화동", "TOT_LVPOP_CO": "5000"}]
    with patch("httpx.get", return_value=_make_response(SVC_FOOT_TRAFFIC, rows)):
        result = _get(SVC_FOOT_TRAFFIC, 1, 1000)
    assert result["list_total_count"] == 1
    assert result["row"] == rows


def test_get_raises_on_api_error():
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {
        SVC_FOOT_TRAFFIC: {
            "result": {"code": "ERROR-500", "message": "서버 오류"},
        }
    }
    with patch("httpx.get", return_value=mock):
        with pytest.raises(RuntimeError, match="API error"):
            _get(SVC_FOOT_TRAFFIC, 1, 1000)


def test_get_raises_on_unexpected_shape():
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {"WRONG_KEY": {}}
    with patch("httpx.get", return_value=mock):
        with pytest.raises(ValueError, match="Unexpected response shape"):
            _get(SVC_FOOT_TRAFFIC, 1, 1000)


# ── fetch_foot_traffic ────────────────────────────────────────────────────────

def test_fetch_foot_traffic_filters_by_district_and_neighborhood(tmp_path, monkeypatch):
    monkeypatch.setattr("src.pipeline.fetch.CACHE_DIR", tmp_path)
    rows = [
        {"GU_NM": "마포구", "DONG_NM": "도화동", "TOT_LVPOP_CO": "5000"},
        {"GU_NM": "마포구", "DONG_NM": "공덕동", "TOT_LVPOP_CO": "3000"},  # different dong
        {"GU_NM": "용산구", "DONG_NM": "도화동", "TOT_LVPOP_CO": "2000"},  # different gu
    ]
    with patch("httpx.get", return_value=_make_response(SVC_FOOT_TRAFFIC, rows)):
        result = fetch_foot_traffic("마포구", "도화동")
    assert len(result) == 1
    assert result[0]["TOT_LVPOP_CO"] == "5000"


def test_fetch_foot_traffic_uses_cache(tmp_path, monkeypatch):
    monkeypatch.setattr("src.pipeline.fetch.CACHE_DIR", tmp_path)
    cached = [{"GU_NM": "마포구", "DONG_NM": "도화동", "TOT_LVPOP_CO": "9999"}]
    cache_file = tmp_path / f"{SVC_FOOT_TRAFFIC}_마포구_도화동.json"
    cache_file.write_text(json.dumps(cached), encoding="utf-8")

    with patch("httpx.get") as mock_get:
        result = fetch_foot_traffic("마포구", "도화동")
        mock_get.assert_not_called()
    assert result == cached


# ── fetch_card_payment ────────────────────────────────────────────────────────

def test_fetch_card_payment_returns_rows(tmp_path, monkeypatch):
    from src.pipeline.fetch import SVC_CARD_PAYMENT
    monkeypatch.setattr("src.pipeline.fetch.CACHE_DIR", tmp_path)
    rows = [{"GU_NM": "마포구", "DONG_NM": "도화동", "THSMON_SELNG_AMT": "10000000"}]
    with patch("httpx.get", return_value=_make_response(SVC_CARD_PAYMENT, rows)):
        result = fetch_card_payment("마포구", "도화동")
    assert len(result) == 1


# ── fetch_commercial_district ─────────────────────────────────────────────────

def test_fetch_commercial_district_returns_rows(tmp_path, monkeypatch):
    from src.pipeline.fetch import SVC_COMMERCIAL_DISTRICT
    monkeypatch.setattr("src.pipeline.fetch.CACHE_DIR", tmp_path)
    rows = [{"GU_NM": "마포구", "DONG_NM": "도화동", "SVC_INDUTY_NM": "일식", "STOR_CO": "12"}]
    with patch("httpx.get", return_value=_make_response(SVC_COMMERCIAL_DISTRICT, rows)):
        result = fetch_commercial_district("마포구", "도화동")
    assert result[0]["SVC_INDUTY_NM"] == "일식"
