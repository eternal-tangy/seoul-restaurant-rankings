"""
Score and rank restaurant categories within a (구, 동, category) combination.

Typical usage:
    from src.pipeline.build import build_dataset
    from src.scoring.score import rank

    df = build_dataset("마포구", "도화동")
    top = rank(df, district="마포구", neighborhood="도화동", category="일식")
"""

import pandas as pd

from src.pipeline.build import build_dataset

# Default weights when all three sources are available (must sum to 1.0)
WEIGHTS = {
    "foot_traffic":       0.4,
    "card_payment":       0.4,
    "commercial_density": 0.2,
}


def _active_weights(available_sources: str) -> dict[str, float]:
    """Redistribute weights evenly across available sources."""
    available = [s.strip() for s in available_sources.split(",") if s.strip()]
    if not available or available == ["none"]:
        return WEIGHTS  # fallback — scores will all be 0 anyway
    base = {k: WEIGHTS[k] for k in available if k in WEIGHTS}
    total = sum(base.values())
    return {k: v / total for k, v in base.items()}


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a composite `score` column and return rows sorted descending.

    Weights are redistributed dynamically based on the `available_sources`
    column so missing APIs don't zero-out all scores.
    """
    df = df.copy()
    sources = df["available_sources"].iloc[0] if "available_sources" in df.columns else ""
    w = _active_weights(sources)

    df["score"] = sum(
        df[col] * weight for col, weight in w.items() if col in df.columns
    )
    return df.sort_values("score", ascending=False).reset_index(drop=True)


def top5(district: str, neighborhood: str, category: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter an already-built DataFrame to (district, neighborhood, category)
    and return the top 5 rows by composite score.

    Use this when you already have a DataFrame from build_dataset().
    """
    filtered = df[
        (df["district"] == district)
        & (df["neighborhood"] == neighborhood)
        & (df["category"] == category)
    ]
    return compute_scores(filtered).head(5)


def rank(
    df: pd.DataFrame,
    district: str,
    neighborhood: str,
    category: str,
) -> pd.DataFrame:
    """
    Filter df to the given location + category, score, and return top 5.
    Raises ValueError if no matching rows are found.
    """
    result = top5(district, neighborhood, category, df)
    if result.empty:
        available = sorted(df["category"].unique())
        raise ValueError(
            f"No data for category '{category}' in {neighborhood}, {district}.\n"
            f"Available categories: {available}"
        )
    return result


def rank_from_api(district: str, neighborhood: str, category: str) -> pd.DataFrame:
    """
    Full pipeline in one call: fetch → normalize → merge → score.
    Returns a scored, ranked DataFrame for the top 5 in the given location + category.
    """
    df = build_dataset(district, neighborhood)
    return rank(df, district, neighborhood, category)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Rank restaurant categories for a dong.")
    parser.add_argument("--district",     required=True, help="구 이름 e.g. 마포구")
    parser.add_argument("--neighborhood", required=True, help="동 이름 e.g. 도화동")
    parser.add_argument("--category",     required=True, help="업종명 e.g. 일식")
    args = parser.parse_args()

    try:
        result = rank_from_api(args.district, args.neighborhood, args.category)
        print(result.to_string(index=False))
    except ValueError as exc:
        print(f"Error: {exc}")
