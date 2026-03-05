"""
AGPARS Cross-Source Linker

Detects probable duplicate listings across different sources
using address normalization and multi-field similarity.

Covers T076.
"""

import re
from difflib import SequenceMatcher

from packages.observability.logger import get_logger

logger = get_logger(__name__)

# Address normalization replacements
ADDRESS_REPLACEMENTS = {
    r"\bstreet\b": "st",
    r"\bst\.": "st",
    r"\broad\b": "rd",
    r"\brd\.": "rd",
    r"\bavenue\b": "ave",
    r"\bave\.": "ave",
    r"\bdrive\b": "dr",
    r"\bdr\.": "dr",
    r"\bplace\b": "pl",
    r"\bpl\.": "pl",
    r"\bapartment\b": "apt",
    r"\bapt\.": "apt",
    r"\bsquare\b": "sq",
    r"\bsq\.": "sq",
    r"'": "",
    r"\.": "",
}

SIMILARITY_THRESHOLD = 0.75
LINK_THRESHOLD = 0.90
MIN_ADDRESS_SIMILARITY = 0.70


def normalize_address(address: str) -> str:
    """
    Normalize an address for comparison.

    Lowercases, strips punctuation, normalizes abbreviations.

    Returns:
        Normalized address string
    """
    if not address:
        return ""

    result = address.lower().strip()

    for pattern, replacement in ADDRESS_REPLACEMENTS.items():
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

    # Remove extra whitespace
    result = re.sub(r"\s+", " ", result).strip()

    return result


def compute_similarity(listing_a: dict, listing_b: dict) -> float:
    """
    Compute similarity score between two listings.

    Uses weighted combination of:
    - Address similarity (0.4)
    - Price proximity (0.25)
    - Beds match (0.15)
    - Location match (0.2)

    Returns:
        Score from 0.0 to 1.0
    """
    score = 0.0

    # Address similarity (weight: 0.4)
    addr_a = normalize_address(listing_a.get("area_text", ""))
    addr_b = normalize_address(listing_b.get("area_text", ""))
    if addr_a and addr_b:
        addr_score = SequenceMatcher(None, addr_a, addr_b).ratio()
        score += addr_score * 0.4
    elif not addr_a and not addr_b:
        score += 0.0  # No address = can't compare

    # Price proximity (weight: 0.25)
    price_a = listing_a.get("price")
    price_b = listing_b.get("price")
    if price_a and price_b and price_a > 0 and price_b > 0:
        price_ratio = min(price_a, price_b) / max(price_a, price_b)
        if price_ratio >= 0.95:
            score += 0.25
        elif price_ratio >= 0.90:
            score += 0.15
        elif price_ratio >= 0.80:
            score += 0.05

    # Beds match (weight: 0.15)
    beds_a = listing_a.get("beds")
    beds_b = listing_b.get("beds")
    if beds_a is not None and beds_b is not None:
        if beds_a == beds_b:
            score += 0.15
    elif beds_a is None and beds_b is None:
        # Both unknown — partial credit
        score += 0.05

    # Location match (weight: 0.2)
    county_a = (listing_a.get("county") or "").lower()
    county_b = (listing_b.get("county") or "").lower()
    city_a = (listing_a.get("city") or "").lower()
    city_b = (listing_b.get("city") or "").lower()

    county_match = county_a and county_a == county_b
    city_match = city_a and city_a == city_b

    if city_match:
        score += 0.2
    elif county_match:
        score += 0.1

    return round(score, 3)


def should_link(listing_a: dict, listing_b: dict) -> bool:
    """
    Determine if two listings should be linked as cross-source duplicates.

    Only links listings from DIFFERENT sources above the similarity threshold
    AND with minimum address similarity to avoid false positives.

    Returns:
        True if the listings should be linked
    """
    # Same source = not cross-source
    if listing_a.get("source") == listing_b.get("source"):
        return False

    # Require minimum address similarity to prevent
    # city+price+beds from matching unrelated listings
    addr_a = normalize_address(listing_a.get("area_text", ""))
    addr_b = normalize_address(listing_b.get("area_text", ""))
    if not addr_a or not addr_b:
        return False
    addr_sim = SequenceMatcher(None, addr_a, addr_b).ratio()
    if addr_sim < MIN_ADDRESS_SIMILARITY:
        return False

    score = compute_similarity(listing_a, listing_b)
    return score >= LINK_THRESHOLD


def find_cross_source_matches(
    new_listing: dict,
    existing_listings: list[dict],
) -> list[dict]:
    """
    Find potential cross-source matches for a new listing.

    Args:
        new_listing: The listing to check
        existing_listings: Pool of existing listings to match against

    Returns:
        List of matching listings with similarity scores
    """
    matches = []

    for existing in existing_listings:
        if not should_link(new_listing, existing):
            continue

        score = compute_similarity(new_listing, existing)
        matches.append({
            "listing": existing,
            "score": score,
        })

    # Sort by score descending
    matches.sort(key=lambda m: m["score"], reverse=True)
    return matches
