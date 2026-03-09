"""
Orchestrate fetch → normalize → merge into a single scored DataFrame.

Sources are fetched independently. If card payment or commercial district
APIs are unavailable, the pipeline degrades gracefully using available data.

Usage:
    from src.pipeline.build import build_dataset
    df = build_dataset("마포구", "도화동")
"""

import pandas as pd

from src.pipeline.fetch import (
    fetch_card_payment,
    fetch_commercial_district,
    fetch_foot_traffic,
    get_dong_code,
)
from src.pipeline.normalize import (
    minmax,
    normalize_card_payment,
    normalize_commercial_district,
    normalize_foot_traffic,
)

# Fallback categories used when commercial district API is unavailable
DEFAULT_CATEGORIES = [
    "일식", "한식", "중식", "양식", "카페",
    "이자카야", "분식", "치킨", "피자", "패스트푸드",
]


def _try_fetch(fn, *args) -> list[dict]:
    """Call a fetcher and return [] on any error, with a warning."""
    try:
        return fn(*args)
    except Exception as exc:
        print(f"  [warn] {fn.__name__} unavailable: {exc}")
        return []


def build_dataset(district: str, neighborhood: str) -> pd.DataFrame:
    """
    Fetch available data sources, normalize, merge, and scale to [0, 1].

    Degrades gracefully when APIs are down:
      - foot traffic only  → scores based on foot traffic
      - + card payment     → adds card payment signal
      - + commercial       → adds real categories and store density

    Returns a DataFrame with one row per category, columns:
        district, neighborhood, category, store_count,
        foot_traffic, card_payment, commercial_density, available_sources
    """
    # 1. Fetch (non-fatal)
    raw_foot       = fetch_foot_traffic(district, neighborhood)
    raw_card       = _try_fetch(fetch_card_payment, district, neighborhood)
    raw_commercial = _try_fetch(fetch_commercial_district, district, neighborhood)

    # 2. Normalize
    foot = normalize_foot_traffic(raw_foot)
    card = normalize_card_payment(raw_card)
    comm = normalize_commercial_district(raw_commercial)

    # 3. Build base DataFrame from commercial data or fallback categories
    if not comm.empty:
        df = comm.copy()
    else:
        print(f"  [warn] No commercial district data — using default categories.")
        df = pd.DataFrame({
            "district":     district,
            "neighborhood": neighborhood,
            "category":     DEFAULT_CATEGORIES,
            "store_count":  0,
            "commercial_density": 0.0,
        })

    # 4. Broadcast dong-level signals to all category rows
    dong_code = get_dong_code(district, neighborhood)

    if not foot.empty and dong_code:
        foot_val = foot.loc[foot["dong_code"] == dong_code, "foot_traffic"]
        df["foot_traffic"] = float(foot_val.iloc[0]) if not foot_val.empty else 0.0
    else:
        df["foot_traffic"] = 0.0

    if not card.empty:
        card_val = card.loc[
            (card["district"] == district) & (card["neighborhood"] == neighborhood),
            "card_payment",
        ]
        df["card_payment"] = float(card_val.iloc[0]) if not card_val.empty else 0.0
    else:
        df["card_payment"] = 0.0

    # 5. Scale signals to [0, 1]
    # - Foot traffic and card payment are dong-level constants (same for all categories).
    #   Scale against an absolute Seoul reference so the value is meaningful.
    # - Commercial density varies per category, so use min-max within the dataset.
    FOOT_TRAFFIC_REF  = 50_000.0   # ~busy Seoul dong daily population
    CARD_PAYMENT_REF  = 5_000_000_000.0  # ~5B KRW monthly card spend

    raw_foot_val = df["foot_traffic"].iloc[0]
    raw_card_val = df["card_payment"].iloc[0]

    df["foot_traffic"]       = min(raw_foot_val / FOOT_TRAFFIC_REF, 1.0)
    df["card_payment"]       = min(raw_card_val / CARD_PAYMENT_REF, 1.0)
    df["commercial_density"] = minmax(df["commercial_density"])

    # 6. Track which sources are available (for score weighting)
    available = []
    if raw_foot_val > 0:                    available.append("foot_traffic")
    if raw_card_val > 0:                    available.append("card_payment")
    if df["commercial_density"].sum() > 0:  available.append("commercial_density")
    df["available_sources"] = ",".join(available) if available else "none"

    return df[["district", "neighborhood", "category", "store_count",
               "foot_traffic", "card_payment", "commercial_density",
               "available_sources"]]
