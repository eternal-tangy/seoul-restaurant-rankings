"""
Kakao Local API client for restaurant search.

Provides:
  - get_dong_coordinates()   : dong name -> (x, y) coordinates
  - search_restaurants()     : returns ranked list of restaurants in a dong by category

Auth: REST API key via Authorization: KakaoAK {key}
Docs: https://developers.kakao.com/docs/latest/ko/local/dev-guide
"""

import os
import time
from dataclasses import dataclass

import httpx
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://dapi.kakao.com/v2/local"

# Kakao category codes
CATEGORY_RESTAURANT = "FD6"
CATEGORY_CAFE       = "CE7"

# Search radius around dong center in meters
DEFAULT_RADIUS = 800

# Maps our category names to Kakao keyword queries
CATEGORY_QUERY_MAP: dict[str, str] = {
    "일식":    "일식",
    "한식":    "한식",
    "중식":    "중식",
    "양식":    "양식",
    "카페":    "카페",
    "이자카야": "이자카야",
    "분식":    "분식",
    "치킨":    "치킨",
    "피자":    "피자",
    "패스트푸드": "패스트푸드",
}


@dataclass
class KakaoPlace:
    name: str
    address: str
    road_address: str
    phone: str
    category: str
    x: float   # longitude
    y: float   # latitude
    place_url: str
    distance: int  # metres from dong center


def _headers() -> dict:
    key = os.getenv("KAKAO_API_KEY", "")
    if not key:
        raise EnvironmentError("KAKAO_API_KEY not set in .env")
    return {"Authorization": f"KakaoAK {key}"}


def _get(endpoint: str, params: dict, retries: int = 3) -> dict:
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(1, retries + 1):
        try:
            r = httpx.get(url, headers=_headers(), params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as exc:
            if attempt == retries:
                raise
            time.sleep(1.5)


# ── Core lookups ──────────────────────────────────────────────────────────────

def get_dong_coordinates(district: str, neighborhood: str) -> tuple[float, float]:
    """
    Convert a district + neighborhood name to (longitude, latitude).
    Uses Kakao address search on "서울 {district} {neighborhood}".
    Returns (x, y) = (longitude, latitude) in WGS84.
    """
    query = f"서울 {district} {neighborhood}"
    body = _get("search/address.json", {"query": query, "size": 1})
    docs = body.get("documents", [])
    if not docs:
        raise ValueError(f"Could not find coordinates for: {query}")
    doc = docs[0]
    x = float(doc.get("x") or doc["address"]["x"])
    y = float(doc.get("y") or doc["address"]["y"])
    return x, y


def search_restaurants(
    district: str,
    neighborhood: str,
    category: str,
    radius: int = DEFAULT_RADIUS,
    max_results: int = 15,
) -> list[KakaoPlace]:
    """
    Search for restaurants of a given category in a dong.

    Strategy:
      1. Get dong coordinates via address search
      2. Keyword search: "{neighborhood} {category}" within radius
         filtered to FD6 (음식점) or CE7 (카페)
      3. Return up to max_results places sorted by distance

    Args:
        district:     구 이름 e.g. 마포구
        neighborhood: 동 이름 e.g. 도화동
        category:     업종명 e.g. 일식
        radius:       search radius in metres (default 800m)
        max_results:  cap on returned places (max 15 per Kakao page)

    Returns:
        List of KakaoPlace sorted by distance from dong center.
    """
    x, y = get_dong_coordinates(district, neighborhood)

    query_term = CATEGORY_QUERY_MAP.get(category, category)
    query = f"{neighborhood} {query_term}"

    cat_code = CATEGORY_CAFE if category == "카페" else CATEGORY_RESTAURANT

    params = {
        "query": query,
        "category_group_code": cat_code,
        "x": x,
        "y": y,
        "radius": radius,
        "size": min(max_results, 15),
        "sort": "distance",
    }

    body = _get("search/keyword.json", params)
    docs = body.get("documents", [])

    return [
        KakaoPlace(
            name         = d["place_name"],
            address      = d.get("address_name", ""),
            road_address = d.get("road_address_name", ""),
            phone        = d.get("phone", ""),
            category     = d.get("category_name", ""),
            x            = float(d["x"]),
            y            = float(d["y"]),
            place_url    = d.get("place_url", ""),
            distance     = int(d.get("distance") or 0),
        )
        for d in docs
    ]
