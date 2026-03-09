"""Tests for src/pipeline/kakao.py — uses mocked HTTP responses."""
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.kakao import (
    KakaoPlace,
    get_dong_coordinates,
    search_restaurants,
)


def _mock_address_response(x="126.9500", y="37.5400"):
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {
        "documents": [{"x": x, "y": y, "address": {"x": x, "y": y}}],
        "meta": {"total_count": 1},
    }
    return mock


def _mock_keyword_response(places: list[dict]):
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {
        "documents": places,
        "meta": {"total_count": len(places), "is_end": True},
    }
    return mock


def _place(name="스시야", dist=100):
    return {
        "place_name": name,
        "address_name": "서울 마포구 도화동 12-3",
        "road_address_name": "서울 마포구 도화로 12",
        "phone": "02-1234-5678",
        "category_name": "음식점 > 일식",
        "category_group_code": "FD6",
        "x": "126.950",
        "y": "37.540",
        "place_url": "https://place.kakao.com/123",
        "distance": str(dist),
    }


# ── get_dong_coordinates ──────────────────────────────────────────────────────

def test_get_dong_coordinates_returns_floats(monkeypatch):
    monkeypatch.setenv("KAKAO_API_KEY", "test_key")
    with patch("httpx.get", return_value=_mock_address_response("126.95", "37.54")):
        x, y = get_dong_coordinates("마포구", "도화동")
    assert isinstance(x, float)
    assert isinstance(y, float)
    assert x == pytest.approx(126.95)
    assert y == pytest.approx(37.54)


def test_get_dong_coordinates_raises_on_empty(monkeypatch):
    monkeypatch.setenv("KAKAO_API_KEY", "test_key")
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {"documents": [], "meta": {"total_count": 0}}
    with patch("httpx.get", return_value=mock):
        with pytest.raises(ValueError, match="Could not find coordinates"):
            get_dong_coordinates("마포구", "없는동")


# ── search_restaurants ────────────────────────────────────────────────────────

def test_search_restaurants_returns_kakao_places(monkeypatch):
    monkeypatch.setenv("KAKAO_API_KEY", "test_key")
    responses = [
        _mock_address_response(),
        _mock_keyword_response([_place("스시야", 50), _place("오마카세", 200)]),
    ]
    with patch("httpx.get", side_effect=responses):
        places = search_restaurants("마포구", "도화동", "일식")
    assert len(places) == 2
    assert all(isinstance(p, KakaoPlace) for p in places)
    assert places[0].name == "스시야"


def test_search_restaurants_parses_fields(monkeypatch):
    monkeypatch.setenv("KAKAO_API_KEY", "test_key")
    responses = [
        _mock_address_response(),
        _mock_keyword_response([_place("스시야", 100)]),
    ]
    with patch("httpx.get", side_effect=responses):
        places = search_restaurants("마포구", "도화동", "일식")
    p = places[0]
    assert p.name == "스시야"
    assert p.road_address == "서울 마포구 도화로 12"
    assert p.phone == "02-1234-5678"
    assert p.distance == 100


def test_search_restaurants_missing_api_key(monkeypatch):
    monkeypatch.setenv("KAKAO_API_KEY", "")
    with pytest.raises(EnvironmentError, match="KAKAO_API_KEY"):
        search_restaurants("마포구", "도화동", "일식")


def test_search_restaurants_uses_cafe_code_for_cafe(monkeypatch):
    monkeypatch.setenv("KAKAO_API_KEY", "test_key")
    captured = []

    def fake_get(url, **kwargs):
        captured.append(kwargs.get("params", {}))
        if "address" in url:
            return _mock_address_response()
        return _mock_keyword_response([_place("카페봄", 50)])

    with patch("httpx.get", side_effect=fake_get):
        search_restaurants("마포구", "도화동", "카페")

    keyword_params = captured[1]
    assert keyword_params.get("category_group_code") == "CE7"
