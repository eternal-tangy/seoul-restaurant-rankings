"""
Generate YouTube video scripts for top 5 restaurant rankings.

The pipeline (fetch → normalize → score) provides area-level signals.
Individual restaurant names/addresses can be supplied via --restaurants
or added manually to the placeholder script afterwards.

Run with:
    python -m src.scripts.generate --district 마포구 --neighborhood 도화동 --category 일식
    python -m src.scripts.generate --district 마포구 --neighborhood 도화동 --category 일식 \\
        --restaurants "스시야|도화동 12-3" "오마카세 도화|도화동 45-6"
"""

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.scoring.score import rank, rank_from_api

OUTPUTS_DIR = Path("outputs")


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class Restaurant:
    name: str
    address: str


# ── Templates ─────────────────────────────────────────────────────────────────

SCRIPT_TEMPLATE = """\
# Top 5 {category} Spots in {neighborhood}, {district}, Seoul

---

Hey everyone! Planning a food trip to Seoul? You've come to the right place!
Today we're revealing the **Top 5 {category} restaurants in {neighborhood}, {district}** —
a neighbourhood packed with amazing eats just waiting to be discovered.

These picks aren't just guesswork — they're ranked using real data: foot traffic,
card payment trends, and commercial district analysis straight from Seoul's open data.
So you know these places are the real deal!

Let's get into it!

---

{entries}

---

There you have it — the **Top 5 {category} spots in {neighborhood}, {district}**!
Whether you're a first-time visitor or a Seoul regular, these places are absolutely
worth a visit. Drop a comment below and let us know which one you tried!

If you found this helpful, please like and subscribe — we post new neighbourhood
food guides every week. See you in the next one, and happy eating in Seoul!

---
*Rankings based on Seoul Open Data Portal (서울 열린데이터광장) — \
foot traffic 40%, card payment volume 40%, commercial density 20%*
"""

ENTRY_TEMPLATE = """\
## #{rank} — {name}
**Where to find it:** {address}
**Why you should go:** {rationale}
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_rationale(row: pd.Series) -> str:
    """Visitor-friendly description of the signals that drove this category's score."""
    foot  = row["foot_traffic"]
    card  = row["card_payment"]
    dens  = row["commercial_density"]
    score = row["score"]
    stores = int(row["store_count"])

    def crowd(v: float) -> str:
        if v >= 0.75: return "always buzzing with locals"
        if v >= 0.4:  return "consistently busy"
        return "a quieter, more relaxed spot"

    def spend(v: float) -> str:
        if v >= 0.75: return "locals love spending here — a strong sign of quality"
        if v >= 0.4:  return "solid card payment activity in the area"
        return "lighter foot spend, but still worth checking out"

    def scene(v: float, n: int) -> str:
        if v >= 0.75: return f"a thriving {n}-restaurant scene with high turnover energy"
        if v >= 0.4:  return f"a well-established area with {n} options to explore"
        return f"a smaller, more intimate dining scene ({n} spots)"

    return (
        f"Overall score: {score:.2f}/1.00. "
        f"This area is {crowd(foot)}, "
        f"{spend(card)}, "
        f"and has {scene(dens, stores)}."
    )


def _make_entries(ranked: pd.DataFrame, restaurants: list[Restaurant] | None) -> str:
    entries = []
    for i, (_, row) in enumerate(ranked.iterrows()):
        if restaurants and i < len(restaurants):
            name    = restaurants[i].name
            address = restaurants[i].address
        else:
            name    = f"[Restaurant {i + 1} — fill in name]"
            address = f"[Address — fill in address]"

        entries.append(
            ENTRY_TEMPLATE.format(
                rank=i + 1,
                name=name,
                address=address,
                rationale=_build_rationale(row),
            )
        )
    return "\n".join(entries)


# ── Public API ────────────────────────────────────────────────────────────────

def generate_script(
    district: str,
    neighborhood: str,
    category: str,
    df: pd.DataFrame,
    restaurants: list[Restaurant] | None = None,
) -> str:
    """
    Generate a video script from an already-built DataFrame.

    Args:
        district:     구 이름 e.g. 마포구
        neighborhood: 동 이름 e.g. 도화동
        category:     업종명 e.g. 일식
        df:           DataFrame from build_dataset() — already fetched and normalized
        restaurants:  Optional list of Restaurant(name, address) in desired order.
                      If fewer than 5 are given, remaining entries use placeholders.
    """
    ranked = rank(df, district, neighborhood, category)
    entries = _make_entries(ranked, restaurants)
    return SCRIPT_TEMPLATE.format(
        category=category,
        neighborhood=neighborhood,
        district=district,
        entries=entries,
    )


def generate_script_from_api(
    district: str,
    neighborhood: str,
    category: str,
    restaurants: list[Restaurant] | None = None,
) -> str:
    """Full pipeline in one call: fetch → normalize → score → script."""
    ranked = rank_from_api(district, neighborhood, category)
    entries = _make_entries(ranked, restaurants)
    return SCRIPT_TEMPLATE.format(
        category=category,
        neighborhood=neighborhood,
        district=district,
        entries=entries,
    )


def save_script(script: str, district: str, neighborhood: str, category: str) -> Path:
    """Write script to outputs/{district}_{neighborhood}_{category}.md and return the path."""
    OUTPUTS_DIR.mkdir(exist_ok=True)
    filename = f"{district}_{neighborhood}_{category}.md".replace(" ", "_")
    path = OUTPUTS_DIR / filename
    path.write_text(script, encoding="utf-8")
    return path


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_restaurant(value: str) -> Restaurant:
    """Parse 'name|address' into a Restaurant. Address is optional."""
    parts = value.split("|", 1)
    return Restaurant(name=parts[0].strip(), address=parts[1].strip() if len(parts) > 1 else "[address TBD]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a YouTube script for top 5 restaurants.")
    parser.add_argument("--district",     required=True, help="구 이름 e.g. 마포구")
    parser.add_argument("--neighborhood", required=True, help="동 이름 e.g. 도화동")
    parser.add_argument("--category",     required=True, help="업종명 e.g. 일식")
    parser.add_argument(
        "--restaurants", nargs="*", metavar="NAME|ADDRESS",
        help="Up to 5 restaurants as 'name|address' pairs e.g. '스시야|도화동 12-3'",
    )
    parser.add_argument("--save", action="store_true", help="Save script to outputs/ directory")
    args = parser.parse_args()

    restaurants = [_parse_restaurant(r) for r in (args.restaurants or [])]

    try:
        script = generate_script_from_api(
            district=args.district,
            neighborhood=args.neighborhood,
            category=args.category,
            restaurants=restaurants or None,
        )
        print(script)

        if args.save:
            path = save_script(script, args.district, args.neighborhood, args.category)
            print(f"\n✓ Saved to {path}")

    except ValueError as exc:
        print(f"Error: {exc}")
