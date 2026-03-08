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


# ── Card payment (소득·소비) ───────────────────────────────────────────────────

CARD_PAYMENT_FIELDS = {
    "GU_NM":           "district",
    "DONG_NM":         "neighborhood",
    "THSMON_SELNG_AMT": "card_payment",   # 당월 매출 금액
}


def normalize_card_payment(rows: list[dict]) -> pd.DataFrame:
    """
    Sum monthly sales amount across all industry codes for the dong.
    """
    if not rows:
        return pd.DataFrame(columns=["district", "neighborhood", "card_payment"])

    df = pd.DataFrame(rows)
    df = df.rename(columns={k: v for k, v in CARD_PAYMENT_FIELDS.items() if k in df.columns})
    df["card_payment"] = pd.to_numeric(df["card_payment"], errors="coerce").fillna(0)

    return (
        df.groupby(["district", "neighborhood"], as_index=False)["card_payment"]
        .sum()
    )


# ── Commercial district (상권분석 점포) ────────────────────────────────────────

COMMERCIAL_FIELDS = {
    "GU_NM":         "district",
    "DONG_NM":       "neighborhood",
    "SVC_INDUTY_NM": "category",           # 업종명 e.g. 일식, 한식
    "STOR_CO":       "store_count",        # 점포 수
    "OPBIZ_RT":      "open_rate",          # 개업률
    "CLSBIZ_RT":     "close_rate",         # 폐업률
}


def normalize_commercial_district(rows: list[dict]) -> pd.DataFrame:
    """
    Return one row per (district, neighborhood, category) with store activity metrics.
    commercial_density = store_count * (1 + open_rate - close_rate)
    """
    if not rows:
        return pd.DataFrame(columns=["district", "neighborhood", "category", "store_count", "commercial_density"])

    df = pd.DataFrame(rows)
    df = df.rename(columns={k: v for k, v in COMMERCIAL_FIELDS.items() if k in df.columns})

    for col in ["store_count", "open_rate", "close_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0

    df["commercial_density"] = df["store_count"] * (1 + df["open_rate"] - df["close_rate"])

    return (
        df.groupby(["district", "neighborhood", "category"], as_index=False)
        .agg(
            store_count=("store_count", "sum"),
            commercial_density=("commercial_density", "sum"),
        )
    )


# ── Min-max normalizer ────────────────────────────────────────────────────────

def minmax(series: pd.Series) -> pd.Series:
    """Scale a series to [0, 1]. Returns 0 if all values are equal."""
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series([0.0] * len(series), index=series.index)
    return (series - lo) / (hi - lo)
