"""
Fetch data from Seoul Open Data Portal (서울 열린데이터광장).
Run with: python -m src.pipeline.fetch

API URL format:
  http://openapi.seoul.go.kr:8088/{API_KEY}/json/{SERVICE}/{start}/{end}/

Service names (verify these on data.seoul.go.kr after getting your API key):
  - SPOP_LOCAL_RESD_DONG  → 생활인구(유동인구) by 행정동   (OA-14991)
  - VwsmTrdarIncmCnsmpDong → 소득·소비 by 행정동            (OA-22166)
  - VwsmTrdarStorDong      → 점포 by 행정동                 (OA-22172)
"""

import json
import os
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SEOUL_API_KEY", "sample")
BASE_URL = "http://openapi.seoul.go.kr:8088"
PAGE_SIZE = 1000          # max records per request
CACHE_DIR = Path("data/raw")
RETRY_LIMIT = 3
RETRY_DELAY = 2.0         # seconds between retries

# ── Service names ─────────────────────────────────────────────────────────────
# If a service name turns out to be wrong, update only these constants.
SVC_FOOT_TRAFFIC       = "SPOP_LOCAL_RESD_DONG"
SVC_CARD_PAYMENT       = "VwsmTrdarIncmCnsmpDong"
SVC_COMMERCIAL_DISTRICT = "VwsmTrdarStorDong"


# ── Core request helper ───────────────────────────────────────────────────────

def _get(service: str, start: int, end: int) -> dict:
    """Make one paginated request and return the parsed JSON body."""
    url = f"{BASE_URL}/{API_KEY}/json/{service}/{start}/{end}/"
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            response = httpx.get(url, timeout=30)
            response.raise_for_status()
            body = response.json()
            # Seoul API wraps everything under the service name key
            if service not in body:
                raise ValueError(f"Unexpected response shape: {list(body.keys())}")
            result = body[service].get("result", {})
            code = result.get("code", "")
            if not code.startswith("INFO"):
                raise RuntimeError(f"API error {code}: {result.get('message')}")
            return body[service]
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            if attempt == RETRY_LIMIT:
                raise
            time.sleep(RETRY_DELAY)


def _fetch_all(service: str, filters: dict | None = None) -> list[dict]:
    """
    Paginate through all records for a service.
    `filters` keys must match exact field names returned by the API.
    """
    rows = []
    start = 1

    first_page = _get(service, start, PAGE_SIZE)
    total = first_page.get("list_total_count", 0)
    rows.extend(first_page.get("row", []))
    start += PAGE_SIZE

    while start <= total:
        page = _get(service, start, min(start + PAGE_SIZE - 1, total))
        rows.extend(page.get("row", []))
        start += PAGE_SIZE

    if filters:
        for key, value in filters.items():
            rows = [r for r in rows if r.get(key) == value]

    return rows


def _cache_path(service: str, suffix: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{service}_{suffix}.json"


def _load_cache(path: Path) -> list[dict] | None:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def _save_cache(path: Path, rows: list[dict]) -> None:
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Public fetchers ───────────────────────────────────────────────────────────

def fetch_foot_traffic(district: str, neighborhood: str) -> list[dict]:
    """
    Fetch 생활인구(유동인구) data for a given 구 and 동.

    Expected response fields (OA-14991):
      ADSTRD_CODE_SE  행정동 코드
      ADSTRD_NM       행정동 명
      TOT_LVPOP_CO    총 생활인구 수
      ...

    Returns a list of row dicts filtered to the requested district + neighborhood.
    """
    cache = _cache_path(SVC_FOOT_TRAFFIC, f"{district}_{neighborhood}")
    cached = _load_cache(cache)
    if cached is not None:
        return cached

    rows = _fetch_all(SVC_FOOT_TRAFFIC, filters={
        "GU_NM": district,
        "DONG_NM": neighborhood,
    })
    _save_cache(cache, rows)
    return rows


def fetch_card_payment(district: str, neighborhood: str) -> list[dict]:
    """
    Fetch 소득·소비(카드매출) data for a given 구 and 동.

    Expected response fields (OA-22166):
      ADSTRD_CD       행정동 코드
      ADSTRD_NM       행정동 명
      THSMON_SELNG_AMT  당월 매출 금액
      THSMON_SELNG_CO   당월 매출 건수
      ...

    Returns a list of row dicts filtered to the requested district + neighborhood.
    """
    cache = _cache_path(SVC_CARD_PAYMENT, f"{district}_{neighborhood}")
    cached = _load_cache(cache)
    if cached is not None:
        return cached

    rows = _fetch_all(SVC_CARD_PAYMENT, filters={
        "GU_NM": district,
        "DONG_NM": neighborhood,
    })
    _save_cache(cache, rows)
    return rows


def fetch_commercial_district(district: str, neighborhood: str) -> list[dict]:
    """
    Fetch 상권분석(점포-행정동) data for a given 구 and 동.

    Expected response fields (OA-22172):
      ADSTRD_CD       행정동 코드
      ADSTRD_NM       행정동 명
      SVC_INDUTY_CD   서비스 업종 코드
      SVC_INDUTY_NM   서비스 업종 명 (e.g. 일식, 한식 ...)
      STOR_CO         점포 수
      OPBIZ_RT        개업률
      CLSBIZ_RT       폐업률
      ...

    Returns a list of row dicts filtered to the requested district + neighborhood.
    """
    cache = _cache_path(SVC_COMMERCIAL_DISTRICT, f"{district}_{neighborhood}")
    cached = _load_cache(cache)
    if cached is not None:
        return cached

    rows = _fetch_all(SVC_COMMERCIAL_DISTRICT, filters={
        "GU_NM": district,
        "DONG_NM": neighborhood,
    })
    _save_cache(cache, rows)
    return rows


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Seoul Open Data for a district/neighborhood.")
    parser.add_argument("--district",     required=True, help="구 이름 e.g. 마포구")
    parser.add_argument("--neighborhood", required=True, help="동 이름 e.g. 도화동")
    parser.add_argument("--source", choices=["foot", "card", "commercial", "all"], default="all")
    args = parser.parse_args()

    fetchers = {
        "foot":       (fetch_foot_traffic,        "유동인구"),
        "card":       (fetch_card_payment,         "카드소비"),
        "commercial": (fetch_commercial_district,  "상권분석"),
    }
    targets = fetchers if args.source == "all" else {args.source: fetchers[args.source]}

    for key, (fn, label) in targets.items():
        print(f"\n[{label}] fetching {args.neighborhood}, {args.district} ...")
        try:
            rows = fn(args.district, args.neighborhood)
            print(f"  ✓ {len(rows)} rows returned")
            if rows:
                print(f"  Fields: {list(rows[0].keys())}")
        except Exception as exc:
            print(f"  ✗ {exc}")
