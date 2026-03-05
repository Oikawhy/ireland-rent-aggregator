"""
AGPARS County Filter

Filter listings by city within county when source only supports county-level search.
"""

import re

from packages.observability.logger import get_logger
from packages.storage.cities import get_cities_by_county

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# COUNTY FILTER
# ═══════════════════════════════════════════════════════════════════════════════


def filter_by_city_in_county(
    listings: list[dict],
    city_id: int,
    city_name: str,
) -> list[dict]:
    """
    Filter listings that actually match a specific city within a county.

    Used when a source only supports county-level search but we need
    city-level results.

    Args:
        listings: List of listing dicts with area_text field
        city_id: Target city ID
        city_name: Target city name

    Returns:
        Filtered list of listings
    """
    if not listings:
        return []

    matched = []
    city_lower = city_name.lower()

    for listing in listings:
        area_text = listing.get("area_text") or listing.get("location_text") or ""

        if _matches_city(area_text, city_lower):
            listing["city_id"] = city_id
            matched.append(listing)

    logger.info(
        "Filtered by city",
        city=city_name,
        total=len(listings),
        matched=len(matched),
    )

    return matched


def _matches_city(area_text: str, city_name: str) -> bool:
    """Check if area_text contains the city name."""
    if not area_text:
        return False

    area_lower = area_text.lower()

    # Direct substring match
    if city_name in area_lower:
        return True

    # Word boundary match
    pattern = rf"\b{re.escape(city_name)}\b"
    return bool(re.search(pattern, area_lower))


# ═══════════════════════════════════════════════════════════════════════════════
# CITY DISTRIBUTION
# ═══════════════════════════════════════════════════════════════════════════════


def get_city_distribution_in_listings(listings: list[dict], county: str) -> dict[str, int]:
    """
    Analyze city distribution in listings for a county.

    Args:
        listings: List of listings
        county: County name

    Returns:
        Dict mapping city name to count
    """
    cities = get_cities_by_county(county)
    if not cities:
        return {}

    distribution = {}

    for city in cities:
        city_name = city["name"].lower()
        count = 0

        for listing in listings:
            area_text = listing.get("area_text") or ""
            if _matches_city(area_text, city_name):
                count += 1

        if count > 0:
            distribution[city["name"]] = count

    return distribution


def filter_listings_by_cities(
    listings: list[dict],
    city_ids: list[int],
    cities_lookup: dict[int, str],
) -> list[dict]:
    """
    Filter listings matching any of the specified cities.

    Args:
        listings: List of listings
        city_ids: List of target city IDs
        cities_lookup: Dict mapping city_id to city_name

    Returns:
        Filtered list
    """
    if not city_ids or not listings:
        return []

    city_names = {cities_lookup.get(cid, "").lower() for cid in city_ids}
    city_names.discard("")  # Remove empty strings

    matched = []
    for listing in listings:
        area_text = listing.get("area_text") or ""
        for city_name in city_names:
            if _matches_city(area_text, city_name):
                matched.append(listing)
                break

    return matched


# ═══════════════════════════════════════════════════════════════════════════════
# COUNTY DETECTION
# ═══════════════════════════════════════════════════════════════════════════════


IRISH_COUNTIES = [
    "Carlow", "Cavan", "Clare", "Cork", "Donegal",
    "Dublin", "Galway", "Kerry", "Kildare", "Kilkenny",
    "Laois", "Leitrim", "Limerick", "Longford", "Louth",
    "Mayo", "Meath", "Monaghan", "Offaly", "Roscommon",
    "Sligo", "Tipperary", "Waterford", "Westmeath", "Wexford",
    "Wicklow",
]


def detect_county(location_text: str) -> str | None:
    """
    Detect county from location text.

    Args:
        location_text: Raw location string

    Returns:
        County name or None
    """
    if not location_text:
        return None

    text_lower = location_text.lower()

    for county in IRISH_COUNTIES:
        pattern = rf"\b{county.lower()}\b"
        if re.search(pattern, text_lower):
            return county

        # Check with "Co." prefix
        if f"co. {county.lower()}" in text_lower:
            return county
        if f"county {county.lower()}" in text_lower:
            return county

    return None
