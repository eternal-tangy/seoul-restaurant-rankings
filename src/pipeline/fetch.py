"""
Fetch data from Seoul Open Data Portal (서울 열린데이터광장).
Run with: python -m src.pipeline.fetch

API URL format:
  http://openapi.seoul.go.kr:8088/{API_KEY}/json/{SERVICE}/{start}/{end}/

Service names (all confirmed working):
  - SPOP_LOCAL_RESD_DONG    -> 생활인구(유동인구) by 행정동  (OA-14991)
  - VwsmAdstrdNcmCnsmpW     -> 소득.소비 by 행정동            (OA-22166)
  - VwsmAdstrdStorW         -> 점포 by 행정동                 (OA-22172)

Key fields:
  - Foot traffic:  ADSTRD_CODE_SE (dong code), TOT_LVPOP_CO (population count)
  - Card payment:  ADSTRD_CD (dong code), FD_EXPNDTR_TOTAMT (dining expenditure)
  - Commercial:    ADSTRD_CD (dong code), SVC_INDUTY_CD_NM (category), STOR_CO (store count)
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
SVC_FOOT_TRAFFIC        = "SPOP_LOCAL_RESD_DONG"   # OA-14991 confirmed
SVC_CARD_PAYMENT        = "VwsmAdstrdNcmCnsmpW"    # OA-22166 confirmed
SVC_COMMERCIAL_DISTRICT = "VwsmAdstrdStorW"         # OA-22172 confirmed

# ── 행정동 코드 lookup (구+동 name -> ADSTRD_CODE_SE) ─────────────────────────
# All 425 Seoul 행정동 codes — sourced directly from VwsmAdstrdNcmCnsmpW (OA-22166)
# quarter 20253, 100% verified against the Seoul Open Data API.
# Format: "구이름/동이름": "8-digit code"
DONG_CODES: dict[str, str] = {
    # 강남구
    "강남구/개포1동": "11680660", "강남구/개포2동": "11680670", "강남구/개포4동": "11680690",
    "강남구/논현1동": "11680521", "강남구/논현2동": "11680531",
    "강남구/대치1동": "11680600", "강남구/대치2동": "11680610", "강남구/대치4동": "11680630",
    "강남구/도곡1동": "11680655", "강남구/도곡2동": "11680656",
    "강남구/삼성1동": "11680580", "강남구/삼성2동": "11680590",
    "강남구/세곡동":  "11680700", "강남구/수서동":  "11680750", "강남구/신사동":  "11680510",
    "강남구/압구정동": "11680545",
    "강남구/역삼1동": "11680640", "강남구/역삼2동": "11680650",
    "강남구/일원1동": "11680730", "강남구/일원2동": "11680740", "강남구/일원본동": "11680720",
    "강남구/청담동":  "11680565",
    # 강동구
    "강동구/강일동":  "11740515", "강동구/고덕1동": "11740550", "강동구/고덕2동": "11740560",
    "강동구/길동":   "11740685", "강동구/둔촌1동": "11740690", "강동구/둔촌2동": "11740700",
    "강동구/명일1동": "11740530", "강동구/명일2동": "11740540", "강동구/상일동":  "11740520",
    "강동구/성내1동": "11740640", "강동구/성내2동": "11740650", "강동구/성내3동": "11740660",
    "강동구/암사1동": "11740570", "강동구/암사2동": "11740580", "강동구/암사3동": "11740590",
    "강동구/천호1동": "11740600", "강동구/천호2동": "11740610", "강동구/천호3동": "11740620",
    # 강북구
    "강북구/미아동":  "11305535", "강북구/번1동":  "11305595", "강북구/번2동":  "11305603",
    "강북구/번3동":  "11305608", "강북구/삼각산동": "11305575", "강북구/삼양동":  "11305534",
    "강북구/송중동":  "11305545", "강북구/송천동":  "11305555",
    "강북구/수유1동": "11305615", "강북구/수유2동": "11305625", "강북구/수유3동": "11305635",
    "강북구/우이동":  "11305645", "강북구/인수동":  "11305660",
    # 강서구
    "강서구/가양1동": "11500603", "강서구/가양2동": "11500604", "강서구/가양3동": "11500605",
    "강서구/공항동":  "11500620", "강서구/등촌1동": "11500520", "강서구/등촌2동": "11500530",
    "강서구/등촌3동": "11500535", "강서구/발산1동": "11500611",
    "강서구/방화1동": "11500630", "강서구/방화2동": "11500640", "강서구/방화3동": "11500641",
    "강서구/염창동":  "11500510", "강서구/우장산동": "11500615",
    "강서구/화곡1동": "11500540", "강서구/화곡2동": "11500550", "강서구/화곡3동": "11500560",
    "강서구/화곡4동": "11500570", "강서구/화곡6동": "11500591", "강서구/화곡8동": "11500593",
    "강서구/화곡본동": "11500590",
    # 관악구
    "관악구/낙성대동": "11620585", "관악구/난곡동":  "11620775", "관악구/난향동":  "11620715",
    "관악구/남현동":  "11620630", "관악구/대학동":  "11620735", "관악구/미성동":  "11620765",
    "관악구/보라매동": "11620525", "관악구/삼성동":  "11620745", "관악구/서림동":  "11620665",
    "관악구/서원동":  "11620645", "관악구/성현동":  "11620565", "관악구/신림동":  "11620695",
    "관악구/신사동":  "11620685", "관악구/신원동":  "11620655", "관악구/은천동":  "11620605",
    "관악구/인헌동":  "11620625", "관악구/조원동":  "11620725", "관악구/중앙동":  "11620615",
    "관악구/청룡동":  "11620595", "관악구/청림동":  "11620545", "관악구/행운동":  "11620575",
    # 광진구
    "광진구/광장동":  "11215810", "광진구/구의1동": "11215850", "광진구/구의2동": "11215860",
    "광진구/구의3동": "11215870", "광진구/군자동":  "11215730", "광진구/능동":   "11215780",
    "광진구/자양1동": "11215820", "광진구/자양2동": "11215830", "광진구/자양3동": "11215840",
    "광진구/자양4동": "11215847", "광진구/중곡1동": "11215740", "광진구/중곡2동": "11215750",
    "광진구/중곡3동": "11215760", "광진구/중곡4동": "11215770", "광진구/화양동":  "11215710",
    # 구로구
    "구로구/가리봉동": "11530595", "구로구/개봉1동": "11530740", "구로구/개봉2동": "11530750",
    "구로구/개봉3동": "11530760", "구로구/고척1동": "11530720", "구로구/고척2동": "11530730",
    "구로구/구로1동": "11530520", "구로구/구로2동": "11530530", "구로구/구로3동": "11530540",
    "구로구/구로4동": "11530550", "구로구/구로5동": "11530560", "구로구/수궁동":  "11530790",
    "구로구/신도림동": "11530510", "구로구/오류1동": "11530770", "구로구/오류2동": "11530780",
    "구로구/항동":   "11530800",
    # 금천구
    "금천구/가산동":  "11545510", "금천구/독산1동": "11545610", "금천구/독산2동": "11545620",
    "금천구/독산3동": "11545630", "금천구/독산4동": "11545640", "금천구/시흥1동": "11545670",
    "금천구/시흥2동": "11545680", "금천구/시흥3동": "11545690", "금천구/시흥4동": "11545700",
    "금천구/시흥5동": "11545710",
    # 노원구
    "노원구/공릉1동": "11350595", "노원구/공릉2동": "11350600",
    "노원구/상계10동": "11350720", "노원구/상계1동": "11350630", "노원구/상계2동": "11350640",
    "노원구/상계3·4동": "11350665", "노원구/상계5동": "11350670", "노원구/상계6·7동": "11350695",
    "노원구/상계8동": "11350700", "노원구/상계9동": "11350710",
    "노원구/월계1동": "11350560", "노원구/월계2동": "11350570", "노원구/월계3동": "11350580",
    "노원구/중계1동": "11350621", "노원구/중계2·3동": "11350625", "노원구/중계4동": "11350624",
    "노원구/중계본동": "11350619", "노원구/하계1동": "11350611", "노원구/하계2동": "11350612",
    # 도봉구
    "도봉구/도봉1동": "11320521", "도봉구/도봉2동": "11320522",
    "도봉구/방학1동": "11320690", "도봉구/방학2동": "11320700", "도봉구/방학3동": "11320710",
    "도봉구/쌍문1동": "11320660", "도봉구/쌍문2동": "11320670", "도봉구/쌍문3동": "11320680",
    "도봉구/쌍문4동": "11320681", "도봉구/창1동":  "11320511", "도봉구/창2동":  "11320512",
    "도봉구/창3동":  "11320513", "도봉구/창4동":  "11320514", "도봉구/창5동":  "11320515",
    # 동대문구
    "동대문구/답십리1동": "11230600", "동대문구/답십리2동": "11230610",
    "동대문구/용신동": "11230536",
    "동대문구/이문1동": "11230740", "동대문구/이문2동": "11230750",
    "동대문구/장안1동": "11230650", "동대문구/장안2동": "11230660",
    "동대문구/전농1동": "11230560", "동대문구/전농2동": "11230570",
    "동대문구/제기동": "11230545", "동대문구/청량리동": "11230705",
    "동대문구/회기동": "11230710", "동대문구/휘경1동": "11230720", "동대문구/휘경2동": "11230730",
    # 동작구
    "동작구/노량진1동": "11590510", "동작구/노량진2동": "11590520",
    "동작구/대방동":  "11590660",
    "동작구/사당1동": "11590620", "동작구/사당2동": "11590630", "동작구/사당3동": "11590640",
    "동작구/사당4동": "11590650", "동작구/사당5동": "11590651",
    "동작구/상도1동": "11590530", "동작구/상도2동": "11590540",
    "동작구/상도3동": "11590550", "동작구/상도4동": "11590560",
    "동작구/신대방1동": "11590670", "동작구/신대방2동": "11590680", "동작구/흑석동": "11590605",
    # 마포구
    "마포구/공덕동":  "11440565", "마포구/대흥동":  "11440600", "마포구/도화동":  "11440585",
    "마포구/망원1동": "11440690", "마포구/망원2동": "11440700", "마포구/상암동":  "11440740",
    "마포구/서강동":  "11440655", "마포구/서교동":  "11440660",
    "마포구/성산1동": "11440720", "마포구/성산2동": "11440730",
    "마포구/신수동":  "11440630", "마포구/아현동":  "11440555", "마포구/연남동":  "11440710",
    "마포구/염리동":  "11440610", "마포구/용강동":  "11440590", "마포구/합정동":  "11440680",
    # 서대문구
    "서대문구/남가좌1동": "11410690", "서대문구/남가좌2동": "11410700",
    "서대문구/북가좌1동": "11410710", "서대문구/북가좌2동": "11410720",
    "서대문구/북아현동": "11410555", "서대문구/신촌동": "11410585", "서대문구/연희동": "11410615",
    "서대문구/천연동": "11410520", "서대문구/충현동": "11410565",
    "서대문구/홍은1동": "11410660", "서대문구/홍은2동": "11410685",
    "서대문구/홍제1동": "11410620", "서대문구/홍제2동": "11410655", "서대문구/홍제3동": "11410640",
    # 서초구
    "서초구/내곡동":  "11650660",
    "서초구/반포1동": "11650560", "서초구/반포2동": "11650570", "서초구/반포3동": "11650580",
    "서초구/반포4동": "11650581", "서초구/반포본동": "11650550",
    "서초구/방배1동": "11650600", "서초구/방배2동": "11650610", "서초구/방배3동": "11650620",
    "서초구/방배4동": "11650621", "서초구/방배본동": "11650590",
    "서초구/서초1동": "11650510", "서초구/서초2동": "11650520", "서초구/서초3동": "11650530",
    "서초구/서초4동": "11650531", "서초구/양재1동": "11650651", "서초구/양재2동": "11650652",
    "서초구/잠원동":  "11650540",
    # 성동구
    "성동구/금호1가동": "11200590", "성동구/금호2·3가동": "11200615", "성동구/금호4가동": "11200620",
    "성동구/마장동":  "11200540", "성동구/사근동":  "11200550",
    "성동구/성수1가1동": "11200650", "성동구/성수1가2동": "11200660",
    "성동구/성수2가1동": "11200670", "성동구/성수2가3동": "11200690",
    "성동구/송정동":  "11200720", "성동구/옥수동":  "11200645",
    "성동구/왕십리2동": "11200520", "성동구/왕십리도선동": "11200535",
    "성동구/용답동":  "11200790", "성동구/응봉동":  "11200580",
    "성동구/행당1동": "11200560", "성동구/행당2동": "11200570",
    # 성북구
    "성북구/길음1동": "11290660", "성북구/길음2동": "11290685",
    "성북구/돈암1동": "11290580", "성북구/돈암2동": "11290590",
    "성북구/동선동":  "11290575", "성북구/보문동":  "11290610", "성북구/삼선동":  "11290555",
    "성북구/석관동":  "11290810", "성북구/성북동":  "11290525", "성북구/안암동":  "11290600",
    "성북구/월곡1동": "11290715", "성북구/월곡2동": "11290725",
    "성북구/장위1동": "11290760", "성북구/장위2동": "11290770", "성북구/장위3동": "11290780",
    "성북구/정릉1동": "11290620", "성북구/정릉2동": "11290630",
    "성북구/정릉3동": "11290640", "성북구/정릉4동": "11290650", "성북구/종암동":  "11290705",
    # 송파구
    "송파구/가락1동": "11710631", "송파구/가락2동": "11710632", "송파구/가락본동": "11710620",
    "송파구/거여1동": "11710531", "송파구/거여2동": "11710532",
    "송파구/마천1동": "11710540", "송파구/마천2동": "11710550",
    "송파구/문정1동": "11710641", "송파구/문정2동": "11710642",
    "송파구/방이1동": "11710561", "송파구/방이2동": "11710562",
    "송파구/삼전동":  "11710610", "송파구/석촌동":  "11710600",
    "송파구/송파1동": "11710580", "송파구/송파2동": "11710590",
    "송파구/오금동":  "11710570", "송파구/오륜동":  "11710566", "송파구/위례동":  "11710647",
    "송파구/잠실2동": "11710670", "송파구/잠실3동": "11710680", "송파구/잠실4동": "11710690",
    "송파구/잠실6동": "11710710", "송파구/잠실7동": "11710720", "송파구/잠실본동": "11710650",
    "송파구/장지동":  "11710646", "송파구/풍납1동": "11710510", "송파구/풍납2동": "11710520",
    # 양천구
    "양천구/목1동":  "11470510", "양천구/목2동":  "11470520", "양천구/목3동":  "11470530",
    "양천구/목4동":  "11470540", "양천구/목5동":  "11470550",
    "양천구/신월1동": "11470560", "양천구/신월2동": "11470570", "양천구/신월3동": "11470580",
    "양천구/신월4동": "11470590", "양천구/신월5동": "11470600", "양천구/신월6동": "11470610",
    "양천구/신월7동": "11470611",
    "양천구/신정1동": "11470620", "양천구/신정2동": "11470630", "양천구/신정3동": "11470640",
    "양천구/신정4동": "11470650", "양천구/신정6동": "11470670", "양천구/신정7동": "11470680",
    # 영등포구
    "영등포구/당산1동": "11560550", "영등포구/당산2동": "11560560",
    "영등포구/대림1동": "11560700", "영등포구/대림2동": "11560710", "영등포구/대림3동": "11560720",
    "영등포구/도림동": "11560585", "영등포구/문래동": "11560605",
    "영등포구/신길1동": "11560630", "영등포구/신길3동": "11560650", "영등포구/신길4동": "11560660",
    "영등포구/신길5동": "11560670", "영등포구/신길6동": "11560680", "영등포구/신길7동": "11560690",
    "영등포구/양평1동": "11560610", "영등포구/양평2동": "11560620",
    "영등포구/여의동": "11560540", "영등포구/영등포동": "11560535", "영등포구/영등포본동": "11560515",
    # 용산구
    "용산구/남영동":  "11170530", "용산구/보광동":  "11170700", "용산구/서빙고동": "11170690",
    "용산구/용문동":  "11170590", "용산구/용산2가동": "11170520",
    "용산구/원효로1동": "11170560", "용산구/원효로2동": "11170570",
    "용산구/이촌1동": "11170630", "용산구/이촌2동": "11170640",
    "용산구/이태원1동": "11170650", "용산구/이태원2동": "11170660",
    "용산구/청파동":  "11170555", "용산구/한강로동": "11170625",
    "용산구/한남동":  "11170685", "용산구/효창동":  "11170580", "용산구/후암동":  "11170510",
    # 은평구
    "은평구/갈현1동": "11380551", "은평구/갈현2동": "11380552", "은평구/구산동":  "11380560",
    "은평구/녹번동":  "11380510", "은평구/대조동":  "11380570",
    "은평구/불광1동": "11380520", "은평구/불광2동": "11380530",
    "은평구/수색동":  "11380650", "은평구/신사1동": "11380631", "은평구/신사2동": "11380632",
    "은평구/역촌동":  "11380625", "은평구/응암1동": "11380580", "은평구/응암2동": "11380590",
    "은평구/응암3동": "11380600", "은평구/증산동":  "11380640", "은평구/진관동":  "11380690",
    # 종로구
    "종로구/가회동":  "11110600", "종로구/교남동":  "11110580", "종로구/무악동":  "11110570",
    "종로구/부암동":  "11110550", "종로구/사직동":  "11110530", "종로구/삼청동":  "11110540",
    "종로구/숭인1동": "11110700", "종로구/숭인2동": "11110710", "종로구/이화동":  "11110640",
    "종로구/종로1·2·3·4가동": "11110615", "종로구/종로5·6가동": "11110630",
    "종로구/창신1동": "11110670", "종로구/창신2동": "11110680", "종로구/창신3동": "11110690",
    "종로구/청운효자동": "11110515", "종로구/평창동":  "11110560", "종로구/혜화동":  "11110650",
    # 중구
    "중구/광희동":   "11140590", "중구/다산동":   "11140625", "중구/동화동":   "11140665",
    "중구/명동":    "11140550", "중구/소공동":   "11140520", "중구/신당5동":  "11140650",
    "중구/신당동":   "11140615", "중구/약수동":   "11140635", "중구/을지로동":  "11140605",
    "중구/장충동":   "11140580", "중구/중림동":   "11140680", "중구/청구동":   "11140645",
    "중구/필동":    "11140570", "중구/황학동":   "11140670", "중구/회현동":   "11140540",
    # 중랑구
    "중랑구/망우3동": "11260660", "중랑구/망우본동": "11260655",
    "중랑구/면목2동": "11260520", "중랑구/면목3·8동": "11260575", "중랑구/면목4동": "11260540",
    "중랑구/면목5동": "11260550", "중랑구/면목7동": "11260570", "중랑구/면목본동": "11260565",
    "중랑구/묵1동":  "11260620", "중랑구/묵2동":  "11260630",
    "중랑구/상봉1동": "11260580", "중랑구/상봉2동": "11260590",
    "중랑구/신내1동": "11260680", "중랑구/신내2동": "11260690",
    "중랑구/중화1동": "11260600", "중랑구/중화2동": "11260610",
    # Aliases — common neighbourhood names that map to their primary administrative dong
    "강남구/역삼동":   "11680640",  # → 역삼1동
    "성동구/성수동":   "11200650",  # → 성수1가1동
    "용산구/이태원동":  "11170650",  # → 이태원1동
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
    Fetch 소득.소비 data for a given 구 and 동 (OA-22166, VwsmAdstrdNcmCnsmpW).

    Total dataset: ~11,475 rows (424 dongs × ~27 quarters).
    Fetches all rows and filters client-side by ADSTRD_CD (dong code).
    Returns all quarterly rows for the dong; normalize_card_payment picks the latest.

    Response fields:
      ADSTRD_CD           행정동 코드
      STDR_YYQU_CD        기준 연도/분기 (e.g. "20253")
      FD_EXPNDTR_TOTAMT   외식비 지출 총액 (dining-out expenditure)
      EXPNDTR_TOTAMT      총 지출 금액
      MT_AVRG_INCOME_AMT  월평균 소득 금액
    """
    cache = _cache_path(SVC_CARD_PAYMENT, f"{district}_{neighborhood}")
    cached = _load_cache(cache)
    if cached is not None:
        return cached

    dong_code = get_dong_code(district, neighborhood)
    if not dong_code:
        raise ValueError(f"No dong code found for {district}/{neighborhood}.")

    rows = _fetch_all(SVC_CARD_PAYMENT, filters={"ADSTRD_CD": dong_code})
    _save_cache(cache, rows)
    return rows


def fetch_commercial_district(district: str, neighborhood: str) -> list[dict]:
    """
    Fetch 상권분석(점포-행정동) data for a given 구 and 동 (OA-22172, VwsmAdstrdStorW).

    Total dataset: ~950,000 rows (424 dongs × ~100 industry codes × ~27 quarters).
    Fetches first 50,000 rows and filters client-side by ADSTRD_CD (dong code).

    Response fields:
      ADSTRD_CD           행정동 코드
      STDR_YYQU_CD        기준 연도/분기
      SVC_INDUTY_CD_NM    업종명 (e.g. "일식음식점", "한식음식점", "커피-음료")
      STOR_CO             점포 수
      OPBIZ_RT            개업률
      CLSBIZ_RT           폐업률
    """
    cache = _cache_path(SVC_COMMERCIAL_DISTRICT, f"{district}_{neighborhood}")
    cached = _load_cache(cache)
    if cached is not None:
        return cached

    dong_code = get_dong_code(district, neighborhood)
    if not dong_code:
        raise ValueError(f"No dong code found for {district}/{neighborhood}.")

    rows = _fetch_all(SVC_COMMERCIAL_DISTRICT,
                      max_rows=50_000,
                      filters={"ADSTRD_CD": dong_code})
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
