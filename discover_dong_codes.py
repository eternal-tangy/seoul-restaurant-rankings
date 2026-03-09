"""
Discover ADSTRD_CODE_SE (행정동 코드) for Seoul neighborhoods.

Two-step process:
  1. OpenStreetMap Nominatim  "서울 {district} {neighborhood}" -> (lon, lat)
     Free, no API key, max ~1 req/sec.
  2. Kakao /geo/coord2regioncode  (x, y) -> H-type 10-digit code -> first 8 digits = dong code
     Requires KAKAO_API_KEY in .env.
     NOTE: Kakao daily quota resets at midnight KST (UTC+9).
     If you see "not found" for all entries, the quota may be exhausted — retry next day.

Usage:
    python discover_dong_codes.py
    python discover_dong_codes.py --district 강남구    # single district only
    python discover_dong_codes.py --output src/pipeline/fetch.py   # auto-patch DONG_CODES
"""

import argparse
import os
import re
import sys
import time

import httpx
from dotenv import load_dotenv

load_dotenv()

KAKAO_KEY = os.getenv("KAKAO_API_KEY", "")
KAKAO_HEADERS = {"Authorization": f"KakaoAK {KAKAO_KEY}"}
KAKAO_BASE    = "https://dapi.kakao.com/v2/local"

# Nominatim (OpenStreetMap) — free geocoder, no API key needed
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "SeoulRestaurantRankings/1.0"}

# Major Seoul districts and their neighborhoods to discover
DISTRICTS: dict[str, list[str]] = {
    "마포구": ["도화동", "공덕동", "아현동", "서강동", "홍익동", "신수동",
               "현석동", "상암동", "합정동", "망원동", "연남동", "성산동"],
    "강남구": ["역삼동", "삼성동", "논현동", "청담동", "압구정동", "대치동",
               "개포동", "도곡동", "수서동", "일원동", "세곡동", "자곡동"],
    "서초구": ["서초동", "방배동", "반포동", "잠원동", "양재동", "우면동",
               "내곡동", "원지동", "신원동", "염곡동"],
    "송파구": ["잠실동", "석촌동", "송파동", "가락동", "문정동", "장지동",
               "위례동", "오금동", "거여동", "마천동", "방이동", "삼전동"],
    "강서구": ["화곡동", "가양동", "마곡동", "발산동", "등촌동", "가화동",
               "공항동", "방화동", "개화동", "과해동", "오곡동", "오쇠동"],
    "영등포구": ["영등포동", "여의동", "당산동", "합정동", "양평동",
                 "문래동", "대림동", "도림동", "신길동"],
    "용산구": ["이태원동", "한강로동", "원효로동", "청파동", "후암동",
               "서빙고동", "동빙고동", "보광동", "한남동", "갈월동"],
    "중구":   ["명동", "을지로동", "충무로동", "신당동", "다산동",
               "약수동", "청구동", "오장동", "예관동", "주자동"],
    "종로구": ["종로1·2·3·4가동", "종로5·6가동", "청운효자동", "사직동",
               "삼청동", "가회동", "이화동", "혜화동", "창신동", "숭인동"],
    "성동구": ["성수동", "왕십리동", "마장동", "사근동", "행당동",
               "응봉동", "금호동", "옥수동", "하왕십리동", "상왕십리동"],
    "광진구": ["구의동", "자양동", "화양동", "군자동", "광장동",
               "중곡동", "능동동", "면목동", "구의제1동"],
    "성북구": ["성북동", "삼선동", "동선동", "돈암동", "안암동",
               "보문동", "정릉동", "길음동", "종암동", "월곡동"],
    "강북구": ["번동", "수유동", "미아동", "삼각산동", "우이동"],
    "도봉구": ["쌍문동", "방학동", "창동", "도봉동"],
    "노원구": ["중계동", "하계동", "상계동", "공릉동", "월계동"],
    "은평구": ["녹번동", "불광동", "갈현동", "구산동", "대조동",
               "신사동", "증산동", "수색동", "응암동", "역촌동"],
    "서대문구": ["천연동", "북아현동", "신촌동", "대현동", "대신동",
                 "홍제동", "남가좌동", "북가좌동", "홍은동", "연희동"],
    "강동구": ["강일동", "상일동", "고덕동", "암사동", "천호동",
               "성내동", "길동", "둔촌동", "명일동", "구천면동"],
    "동작구": ["노량진동", "상도동", "흑석동", "사당동", "대방동",
               "신대방동", "동작동", "본동", "상도제1동"],
    "관악구": ["신림동", "봉천동", "남현동", "청룡동", "행운동",
               "낙성대동", "인헌동", "서원동", "성현동", "중앙동"],
    "동대문구": ["전농동", "답십리동", "장안동", "청량리동", "용신동",
                 "이문동", "회기동", "휘경동", "제기동"],
    "중랑구": ["면목동", "상봉동", "묵동", "신내동", "망우동", "중화동"],
}


def _kakao_get(endpoint: str, params: dict, retries: int = 3) -> dict:
    url = f"{KAKAO_BASE}/{endpoint}"
    for attempt in range(1, retries + 1):
        try:
            r = httpx.get(url, headers=KAKAO_HEADERS, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429:
                time.sleep(3 * attempt)
                continue
            if attempt == retries:
                raise
            time.sleep(1)
        except httpx.HTTPError:
            if attempt == retries:
                raise
            time.sleep(1)
    return {}


def get_coords_nominatim(district: str, neighborhood: str) -> tuple[float, float] | None:
    """
    Geocode via OpenStreetMap Nominatim — free, no rate quota issues.
    Returns (longitude, latitude) i.e. (x, y).
    Nominatim policy: max 1 req/sec — we sleep 1s between calls.
    """
    query = f"서울 {district} {neighborhood}"
    try:
        r = httpx.get(NOMINATIM_URL,
                      headers=NOMINATIM_HEADERS,
                      params={"q": query, "format": "json", "limit": 1},
                      timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data:
            return None
        return float(data[0]["lon"]), float(data[0]["lat"])  # x=lon, y=lat
    except httpx.HTTPError:
        return None


def get_h_code(x: float, y: float) -> str | None:
    """Kakao coord2regioncode -> H-type 10-digit administrative code."""
    body = _kakao_get("geo/coord2regioncode.json", {"x": x, "y": y})
    docs = body.get("documents", [])
    for doc in docs:
        if doc.get("region_type") == "H":
            return doc.get("code", "")
    return None


def discover(district: str, neighborhood: str) -> str | None:
    """Return the 8-digit ADSTRD_CODE_SE for a single dong, or None."""
    coords = get_coords_nominatim(district, neighborhood)
    if not coords:
        return None
    x, y = coords
    h_code = get_h_code(x, y)
    if not h_code or len(h_code) < 8:
        return None
    return h_code[:8]


def patch_fetch_py(results: dict[str, str], fetch_path: str) -> None:
    """Append newly discovered codes to DONG_CODES dict in fetch.py."""
    content = open(fetch_path, encoding="utf-8").read()
    # Find existing keys
    existing = set(re.findall(r'"([^"]+/[^"]+)":\s*"(\d+)"', content))
    existing_keys = {k for k, _ in existing}

    new_entries = [
        f'    "{k}": "{v}",'
        for k, v in sorted(results.items())
        if k not in existing_keys
    ]
    if not new_entries:
        print("All discovered codes already present in fetch.py")
        return

    # Insert new entries before the closing }
    new_block = "\n".join(new_entries)
    content = re.sub(
        r'(DONG_CODES: dict\[str, str\] = \{[^}]*)(})',
        lambda m: m.group(1) + new_block + "\n" + m.group(2),
        content,
        flags=re.DOTALL,
    )
    with open(fetch_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Patched {len(new_entries)} new entries into {fetch_path}")


def main():
    parser = argparse.ArgumentParser(description="Discover Seoul dong codes via Kakao.")
    parser.add_argument("--district", help="Only discover for this district (e.g. 강남구)")
    parser.add_argument("--output",   help="Path to fetch.py to auto-patch DONG_CODES")
    args = parser.parse_args()

    if not KAKAO_KEY:
        print("ERROR: KAKAO_API_KEY not set in .env (needed for coord2regioncode step)")
        sys.exit(1)

    targets = (
        {args.district: DISTRICTS[args.district]}
        if args.district and args.district in DISTRICTS
        else DISTRICTS
    )

    results: dict[str, str] = {}
    total = sum(len(v) for v in targets.values())
    done = 0

    for district, neighborhoods in targets.items():
        print(f"\n[{district}]")
        for hood in neighborhoods:
            done += 1
            key = f"{district}/{hood}"
            code = discover(district, hood)
            if code:
                results[key] = code
                print(f"  OK  {key}: {code}")
            else:
                print(f"  --  {key}: not found")
            time.sleep(1.1)   # Nominatim policy: max 1 req/sec

    print(f"\nTotal: {len(results)}/{total} found")

    if results:
        print("\nAdd to DONG_CODES in fetch.py:")
        for k, v in sorted(results.items()):
            print(f'    "{k}": "{v}",')

    if args.output and results:
        patch_fetch_py(results, args.output)


if __name__ == "__main__":
    main()
