"""
Normalize raw Seoul Open Data API rows into standard DataFrames.

Each function maps the raw Korean field names from one data source
into a consistent schema that the scoring engine can consume.
"""

import pandas as pd


# ── Foot traffic (유동인구) ────────────────────────────────────────────────────

FOOT_TRAFFIC_FIELDS = {
    "ADSTRD_CODE_SE": "dong_code",    # 행정동 코드
    "TOT_LVPOP_CO":   "foot_traffic", # 총 생활인구 수
}


def normalize_foot_traffic(rows: list[dict]) -> pd.DataFrame:
    """
    Collapse all time-slot rows for a dong into a single daily total.
    The API returns one row per hour per dong code; we sum TOT_LVPOP_CO.
    Result has columns: dong_code, foot_traffic.
    district/neighborhood are added by build.py via dong code lookup.
    """
    if not rows:
        return pd.DataFrame(columns=["dong_code", "foot_traffic"])

    df = pd.DataFrame(rows)
    df = df.rename(columns={k: v for k, v in FOOT_TRAFFIC_FIELDS.items() if k in df.columns})
    df["foot_traffic"] = pd.to_numeric(df["foot_traffic"], errors="coerce").fillna(0)

    return (
        df.groupby("dong_code", as_index=False)["foot_traffic"]
        .sum()
    )


# ── Card payment (소득·소비) OA-22166 ─────────────────────────────────────────
# Uses dining-out expenditure (외식비) as the proxy for restaurant card spend.
# Data has multiple quarterly rows per dong; we use the most recent quarter.

CARD_PAYMENT_FIELDS = {
    "ADSTRD_CD":        "dong_code",
    "STDR_YYQU_CD":     "quarter",
    "FD_EXPNDTR_TOTAMT": "card_payment",   # 외식비 지출 총액 (dining-out expenditure)
}


def normalize_card_payment(rows: list[dict]) -> pd.DataFrame:
    """
    Return one row per dong with the most-recent quarter's dining expenditure.
    """
    if not rows:
        return pd.DataFrame(columns=["dong_code", "card_payment"])

    df = pd.DataFrame(rows)
    df = df.rename(columns={k: v for k, v in CARD_PAYMENT_FIELDS.items() if k in df.columns})
    df["card_payment"] = pd.to_numeric(df["card_payment"], errors="coerce").fillna(0)

    # Keep only the most recent quarter per dong
    latest = df.sort_values("quarter", ascending=False).groupby("dong_code", as_index=False).first()
    return latest[["dong_code", "card_payment"]]


# ── Commercial district (상권분석 점포) OA-22172 ───────────────────────────────
# Industry names like "일식음식점" are mapped to our standard category names.

COMMERCIAL_FIELDS = {
    "ADSTRD_CD":        "dong_code",
    "STDR_YYQU_CD":     "quarter",
    "SVC_INDUTY_CD_NM": "category",    # 업종명 e.g. "일식음식점", "커피-음료"
    "STOR_CO":          "store_count", # 점포 수
    "OPBIZ_RT":         "open_rate",   # 개업률
    "CLSBIZ_RT":        "close_rate",  # 폐업률
}

# Maps API category names → our standard names
CATEGORY_NAME_MAP: dict[str, str] = {
    "한식음식점":  "한식",
    "중식음식점":  "중식",
    "일식음식점":  "일식",
    "양식음식점":  "양식",
    "분식전문점":  "분식",
    "치킨전문점":  "치킨",
    "피자전문점":  "피자",
    "패스트푸드":  "패스트푸드",
    "커피-음료":   "카페",
    "호프-간이주점": "이자카야",
}


def normalize_commercial_district(rows: list[dict]) -> pd.DataFrame:
    """
    Return one row per (dong_code, category) using the most recent quarter.
    commercial_density = store_count * (1 + open_rate - close_rate)
    Categories are mapped to our standard names via CATEGORY_NAME_MAP.
    """
    if not rows:
        return pd.DataFrame(columns=["dong_code", "category", "store_count", "commercial_density"])

    df = pd.DataFrame(rows)
    df = df.rename(columns={k: v for k, v in COMMERCIAL_FIELDS.items() if k in df.columns})

    # Keep only the most recent quarter
    df = df.sort_values("quarter", ascending=False)
    df = df.groupby(["dong_code", "category"], as_index=False).first()

    # Map to standard category names; drop unmapped categories
    df["category"] = df["category"].map(CATEGORY_NAME_MAP)
    df = df.dropna(subset=["category"])

    for col in ["store_count", "open_rate", "close_rate"]:
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

    df["commercial_density"] = df["store_count"] * (1 + df["open_rate"] - df["close_rate"])

    return df[["dong_code", "category", "store_count", "commercial_density"]]


# ── Min-max normalizer ────────────────────────────────────────────────────────

def minmax(series: pd.Series) -> pd.Series:
    """Scale a series to [0, 1]. Returns 0 if all values are equal."""
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series([0.0] * len(series), index=series.index)
    return (series - lo) / (hi - lo)
