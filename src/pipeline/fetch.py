"""
Fetch data from Seoul Open Data Portal (서울 열린데이터광장).
Run with: python -m src.pipeline.fetch

API URL format:
  http://openapi.seoul.go.kr:8088/{API_KEY}/json/{SERVICE}/{start}/{end}/
  http://openapi.seoul.go.kr:8088/{API_KEY}/json/{SERVICE}/{start}/{end}/{PARAM}/

Service names:
  - SPOP_LOCAL_RESD_DONG   -> 생활인구(유동인구) by 행정동  (OA-14991)  [confirmed working]
  - VwsmTrdarIncmCnsmpDong -> 소득.소비 by 행정동            (OA-22166)  [service name TBC]
  - VwsmTrdarStorDong      -> 점포 by 행정동                 (OA-22172)  [service name TBC]

NOTE: OA-22166 and OA-22172 service names return ERROR-500.
To find the correct names, go to:
  https://data.seoul.go.kr/dataList/OA-22166/S/1/datasetView.do  -> Open API tab
  https://data.seoul.go.kr/dataList/OA-22172/S/1/datasetView.do  -> Open API tab
Update SVC_CARD_PAYMENT and SVC_COMMERCIAL_DISTRICT constants below.
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
PAGE_SIZE = 1000
CACHE_DIR = Path("data/raw")
RETRY_LIMIT = 3
RETRY_DELAY = 2.0

# ── Service names ─────────────────────────────────────────────────────────────
SVC_FOOT_TRAFFIC        = "SPOP_LOCAL_RESD_DONG"   # confirmed
SVC_CARD_PAYMENT        = "VwsmTrdarIncmCnsmpDong"  # TODO: verify on portal
SVC_COMMERCIAL_DISTRICT = "VwsmTrdarStorDong"       # TODO: verify on portal

# ── 행정동 코드 lookup (구+동 name -> ADSTRD_CODE_SE) ─────────────────────────
# The foot traffic API filters by 8-digit administrative dong code, not by name.
# Add entries here as you discover them via `python -m src.pipeline.fetch --discover`.
# Format: "구이름/동이름": "코드"
DONG_CODES: dict[str, str] = {
    # ── Verified codes (confirmed against Seoul Open Data API) ──────────────
    # 마포구 (district prefix 11440)
    "마포구/도화동":  "11440680",
    "마포구/공덕동":  "11440720",
    "마포구/아현동":  "11440740",
    "마포구/홍익동":  "11440710",
    "마포구/신수동":  "11440660",
    "마포구/상암동":  "11440600",
    "마포구/합정동":  "11440700",
    "마포구/망원동":  "11440690",
    "마포구/연남동":  "11440730",
    "마포구/성산동":  "11440610",
    # 강남구 (district prefix 11680) — confirmed via Kakao coord2regioncode
    "강남구/역삼동":  "11680640",
    "강남구/삼성동":  "11680590",
    "강남구/논현동":  "11680521",
    "강남구/청담동":  "11680565",
    "강남구/압구정동": "11680545",
    "강남구/대치동":  "11680630",
    "강남구/개포동":  "11680660",
    "강남구/도곡동":  "11680690",
    "강남구/수서동":  "11680750",
    "강남구/일원동":  "11680720",
    "강남구/세곡동":  "11680700",
    "강남구/자곡동":  "11680700",
    # 서초구 (district prefix 11650)
    "서초구/서초동":  "11650520",
    "서초구/방배동":  "11650560",
    "서초구/반포동":  "11650530",
    "서초구/잠원동":  "11650540",
    "서초구/양재동":  "11650580",
    # 송파구 (district prefix 11710)
    "송파구/잠실동":  "11710520",
    "송파구/석촌동":  "11710540",
    "송파구/송파동":  "11710550",
    "송파구/문정동":  "11710570",
    "송파구/장지동":  "11710580",
    "송파구/오금동":  "11710600",
    "송파구/거여동":  "11710610",
    "송파구/마천동":  "11710620",
    # 용산구 (district prefix 11170)
    "용산구/이태원동": "11170630",
    "용산구/한강로동": "11170520",
    "용산구/원효로동": "11170530",
    "용산구/한남동":  "11170660",
    # 종로구 (district prefix 11110)
    "종로구/혜화동":  "11110680",
    "종로구/이화동":  "11110670",
    "종로구/청운효자동": "11110530",
    "종로구/사직동":  "11110540",
    # 성동구 (district prefix 11200)
    "성동구/왕십리동": "11200550",
    "성동구/옥수동":  "11200650",
    # 영등포구 (district prefix 11560)
    "영등포구/여의동": "11560540",
    "영등포구/당산동": "11560550",
    "영등포구/대림동": "11560620",
    # 동작구 (district prefix 11590)
    "동작구/노량진동": "11590520",
    "동작구/상도동":  "11590540",
    "동작구/흑석동":  "11590530",
    # 은평구 (district prefix 11380)
    "은평구/불광동":  "11380570",
    "은평구/신사동":  "11380600",
    "은평구/수색동":  "11380640",
    # 강서구 (district prefix 11500)
    "강서구/가양동":  "11500560",
    "강서구/발산동":  "11500590",
    # 강동구 (district prefix 11740)
    "강동구/천호동":  "11740560",
    "강동구/암사동":  "11740540",
    "강동구/길동":   "11740600",
    "강동구/명일동":  "11740620",
    # 성북구 (district prefix 11290)
    "성북구/성북동":  "11290525",
    "성북구/정릉동":  "11290580",
    "성북구/길음동":  "11290600",
    "성북구/종암동":  "11290610",
    # 노원구 (district prefix 11350)
    "노원구/중계동":  "11350560",
    "노원구/상계동":  "11350580",
}


def get_dong_code(district: str, neighborhood: str) -> str | None:
    return DONG_CODES.get(f"{district}/{neighborhood}")


# ── Core request helper ───────────────────────────────────────────────────────

def _get(service: str, start: int, end: int, url_param: str | None = None) -> dict:
    """Make one paginated request and return the parsed JSON body."""
    param_part = f"/{url_param}" if url_param else ""
    url = f"{BASE_URL}/{API_KEY}/json/{service}/{start}/{end}{param_part}/"

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            response = httpx.get(url, timeout=30)
            response.raise_for_status()
            body = response.json()

            if service not in body:
                raise ValueError(f"Unexpected response shape: {list(body.keys())}")

            inner = body[service]
            # Seoul API uses uppercase RESULT/CODE/MESSAGE
            result = inner.get("RESULT", inner.get("result", {}))
            code   = result.get("CODE", result.get("code", ""))

            if not code.startswith("INFO"):
                msg = result.get("MESSAGE", result.get("message", ""))
                raise RuntimeError(f"API error {code}: {msg}")

            return inner
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            if attempt == RETRY_LIMIT:
                raise
            time.sleep(RETRY_DELAY)


def _fetch_all(service: str, url_param: str | None = None,
               filters: dict | None = None,
               max_rows: int | None = None) -> list[dict]:
    """
    Paginate through records, optionally capped at max_rows.
    Filters are applied client-side after fetching.
    """
    rows: list[dict] = []
    start = 1

    first_page = _get(service, start, PAGE_SIZE, url_param)
    total = first_page.get("list_total_count", 0)
    if max_rows:
        total = min(total, max_rows)
    rows.extend(first_page.get("row", []))
    start += PAGE_SIZE

    while start <= total:
        page = _get(service, start, min(start + PAGE_SIZE - 1, total), url_param)
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

    The API has 641K+ rows across all dongs and dates. We fetch only the first
    FOOT_TRAFFIC_FETCH_ROWS rows (most recent data) and filter by dong code.

    Response fields (OA-14991):
      ADSTRD_CODE_SE  행정동 코드
      STDR_DE_ID      기준 일자
      TMZON_PD_SE     시간대 구분 (00-23)
      TOT_LVPOP_CO    총 생활인구 수
    """
    cache = _cache_path(SVC_FOOT_TRAFFIC, f"{district}_{neighborhood}")
    cached = _load_cache(cache)
    if cached is not None:
        return cached

    dong_code = get_dong_code(district, neighborhood)
    if not dong_code:
        raise ValueError(
            f"No dong code found for {district}/{neighborhood}. "
            f"Run: python -m src.pipeline.fetch --discover --prefix 1144"
        )

    # Fetch first 12,000 rows (covers ~1 full day for all Seoul dongs)
    # and filter client-side by dong code
    rows = _fetch_all(SVC_FOOT_TRAFFIC,
                      max_rows=12_000,
                      filters={"ADSTRD_CODE_SE": dong_code})
    _save_cache(cache, rows)
    return rows


def fetch_card_payment(district: str, neighborhood: str) -> list[dict]:
    """
    Fetch 소득.소비(카드매출) data for a given 구 and 동.
    NOTE: Service name SVC_CARD_PAYMENT may need updating — see module docstring.
    """
    cache = _cache_path(SVC_CARD_PAYMENT, f"{district}_{neighborhood}")
    cached = _load_cache(cache)
    if cached is not None:
        return cached

    dong_code = get_dong_code(district, neighborhood)
    rows = _fetch_all(SVC_CARD_PAYMENT, url_param=dong_code)
    _save_cache(cache, rows)
    return rows


def fetch_commercial_district(district: str, neighborhood: str) -> list[dict]:
    """
    Fetch 상권분석(점포-행정동) data for a given 구 and 동.
    NOTE: Service name SVC_COMMERCIAL_DISTRICT may need updating — see module docstring.
    """
    cache = _cache_path(SVC_COMMERCIAL_DISTRICT, f"{district}_{neighborhood}")
    cached = _load_cache(cache)
    if cached is not None:
        return cached

    dong_code = get_dong_code(district, neighborhood)
    rows = _fetch_all(SVC_COMMERCIAL_DISTRICT, url_param=dong_code)
    _save_cache(cache, rows)
    return rows


# ── Discover dong codes ───────────────────────────────────────────────────────

def discover_dong_codes(district_prefix: str = "1144") -> None:
    """
    Fetch a sample of foot traffic rows and print unique ADSTRD_CODE_SE values
    that start with `district_prefix`. Use this to populate DONG_CODES above.
    Default prefix 1144 = 마포구.
    """
    print(f"Fetching sample rows to discover dong codes for prefix {district_prefix}...")
    first = _get(SVC_FOOT_TRAFFIC, 1, PAGE_SIZE)
    rows = first.get("row", [])
    codes = sorted({r["ADSTRD_CODE_SE"] for r in rows
                    if r.get("ADSTRD_CODE_SE", "").startswith(district_prefix)})
    if codes:
        print(f"Found {len(codes)} dong codes:")
        for c in codes:
            print(f"  {c}")
        print("\nAdd these to DONG_CODES in fetch.py as \"구이름/동이름\": \"코드\"")
    else:
        print(f"No codes found with prefix {district_prefix} in first {PAGE_SIZE} rows. "
              f"Try a different prefix.")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Fetch Seoul Open Data for a district/neighborhood.")
    parser.add_argument("--district",     help="구 이름 e.g. 마포구")
    parser.add_argument("--neighborhood", help="동 이름 e.g. 도화동")
    parser.add_argument("--source",
                        choices=["foot", "card", "commercial", "all"], default="all")
    parser.add_argument("--discover", action="store_true",
                        help="Discover dong codes for a district prefix")
    parser.add_argument("--prefix", default="1144",
                        help="District code prefix for --discover (default: 1144 = 마포구)")
    args = parser.parse_args()

    if args.discover:
        discover_dong_codes(args.prefix)
        sys.exit(0)

    if not args.district or not args.neighborhood:
        parser.error("--district and --neighborhood are required unless using --discover")

    fetchers = {
        "foot":       (fetch_foot_traffic,       "foot traffic"),
        "card":       (fetch_card_payment,        "card payment"),
        "commercial": (fetch_commercial_district, "commercial district"),
    }
    targets = fetchers if args.source == "all" else {args.source: fetchers[args.source]}

    for key, (fn, label) in targets.items():
        print(f"\n[{label}] fetching {args.neighborhood}, {args.district} ...")
        try:
            rows = fn(args.district, args.neighborhood)
            print(f"  OK: {len(rows)} rows returned")
            if rows:
                print(f"  Fields: {list(rows[0].keys())}")
        except Exception as exc:
            print(f"  FAILED: {exc}")
