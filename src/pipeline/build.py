"""
Orchestrate fetch → normalize → merge into a single scored DataFrame.

Usage:
    from src.pipeline.build import build_dataset
    df = build_dataset("마포구", "도화동")
    # df columns: district, neighborhood, category, store_count,
    #             foot_traffic, card_payment, commercial_density
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


def build_dataset(district: str, neighborhood: str) -> pd.DataFrame:
    """
    Fetch all three data sources, normalize, merge, and scale to [0, 1].

    Returns a DataFrame with one row per food category found in the
    commercial district data, ready to pass into the scoring engine.

    Columns:
        district            구 이름
        neighborhood        동 이름
        category            업종명 (e.g. 일식, 한식)
        store_count         raw 점포 수
        foot_traffic        min-max scaled [0, 1]
        card_payment        min-max scaled [0, 1]
        commercial_density  min-max scaled [0, 1]
    """
    # 1. Fetch raw rows
    raw_foot       = fetch_foot_traffic(district, neighborhood)
    raw_card       = fetch_card_payment(district, neighborhood)
    raw_commercial = fetch_commercial_district(district, neighborhood)

    # 2. Normalize to standard schemas
    foot = normalize_foot_traffic(raw_foot)
    card = normalize_card_payment(raw_card)
    comm = normalize_commercial_district(raw_commercial)

    if comm.empty:
        raise ValueError(f"No commercial district data for {neighborhood}, {district}.")

    # 3. Merge — commercial data drives the rows (one per category)
    #    Foot traffic and card payment are dong-level signals broadcast to all categories.
    df = comm.copy()

    if not foot.empty:
        dong_code = get_dong_code(district, neighborhood)
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

    # 4. Min-max scale each signal across categories so scores are comparable
    df["foot_traffic"]       = minmax(df["foot_traffic"])
    df["card_payment"]       = minmax(df["card_payment"])
    df["commercial_density"] = minmax(df["commercial_density"])

    return df[["district", "neighborhood", "category", "store_count",
               "foot_traffic", "card_payment", "commercial_density"]]
