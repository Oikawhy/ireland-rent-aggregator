"""
AGPARS City Synonyms Resolution

Resolve raw location text to city IDs using fuzzy matching.
"""

import re
from functools import lru_cache

from packages.observability.logger import get_logger
from packages.storage.cities import fuzzy_match_city, get_city_by_name

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DUBLIN POSTAL CODES
# ═══════════════════════════════════════════════════════════════════════════════


DUBLIN_POSTAL_CODES = {
    "d1": "Dublin 1",
    "d2": "Dublin 2",
    "d3": "Dublin 3",
    "d4": "Dublin 4",
    "d5": "Dublin 5",
    "d6": "Dublin 6",
    "d6w": "Dublin 6W",
    "d7": "Dublin 7",
    "d8": "Dublin 8",
    "d9": "Dublin 9",
    "d10": "Dublin 10",
    "d11": "Dublin 11",
    "d12": "Dublin 12",
    "d13": "Dublin 13",
    "d14": "Dublin 14",
    "d15": "Dublin 15",
    "d16": "Dublin 16",
    "d17": "Dublin 17",
    "d18": "Dublin 18",
    "d20": "Dublin 20",
    "d22": "Dublin 22",
    "d24": "Dublin 24",
    "dublin1": "Dublin 1",
    "dublin2": "Dublin 2",
    "dublin3": "Dublin 3",
    "dublin4": "Dublin 4",
    "dublin5": "Dublin 5",
    "dublin6": "Dublin 6",
    "dublin7": "Dublin 7",
    "dublin8": "Dublin 8",
    "dublin9": "Dublin 9",
    "dublin10": "Dublin 10",
    "dublin11": "Dublin 11",
    "dublin12": "Dublin 12",
    "dublin13": "Dublin 13",
    "dublin14": "Dublin 14",
    "dublin15": "Dublin 15",
    "dublin16": "Dublin 16",
    "dublin17": "Dublin 17",
    "dublin18": "Dublin 18",
    "dublin20": "Dublin 20",
    "dublin22": "Dublin 22",
    "dublin24": "Dublin 24",
}


# ═══════════════════════════════════════════════════════════════════════════════
# CITY SYNONYMS
# ═══════════════════════════════════════════════════════════════════════════════


# Common synonyms and alternative spellings
CITY_SYNONYMS = {
    # Dublin variations
    "dublin city": "Dublin",
    "dublin city centre": "Dublin",
    "dublin centre": "Dublin",
    "city centre dublin": "Dublin",

    # Cork variations
    "cork city": "Cork",
    "cork city centre": "Cork",

    # Galway variations
    "galway city": "Galway",
    "galway city centre": "Galway",

    # Limerick variations
    "limerick city": "Limerick",
    "limerick city centre": "Limerick",

    # Waterford variations
    "waterford city": "Waterford",

    # Dun Laoghaire variations
    "dun laoghaire": "Dún Laoghaire",
    "dunlaoghaire": "Dún Laoghaire",

    # Bray
    "bray": "Bray",

    # Swords
    "swords": "Swords",

    # Greystones
    "greystones": "Greystones",

    # Maynooth
    "maynooth": "Maynooth",

    # Dundalk
    "dundalk": "Dundalk",

    # Drogheda
    "drogheda": "Drogheda",

    # Killarney
    "killarney": "Killarney",

    # Kilkenny
    "kilkenny": "Kilkenny",

    # Sligo
    "sligo": "Sligo",

    # Athlone
    "athlone": "Athlone",

    # Ennis
    "ennis": "Ennis",

    # Wexford
    "wexford": "Wexford",

    # Tralee
    "tralee": "Tralee",

    # Carlow
    "carlow": "Carlow",

    # Naas
    "naas": "Naas",

    # Navan
    "navan": "Navan",

    # Letterkenny
    "letterkenny": "Letterkenny",

    # Castlebar
    "castlebar": "Castlebar",
}


# ═══════════════════════════════════════════════════════════════════════════════
# RESOLUTION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def resolve_city(location_text: str) -> dict | None:
    """
    Resolve location text to a city record.

    Args:
        location_text: Raw location string

    Returns:
        Dict with city info (id, name, county) or None
    """
    if not location_text:
        return None

    text = location_text.strip().lower()

    # Step 1: Check for Dublin postal codes
    city_name = _resolve_dublin_postal(text)
    if city_name:
        return get_city_by_name(city_name)

    # Step 2: Check synonyms
    city_name = _resolve_synonym(text)
    if city_name:
        return get_city_by_name(city_name)

    # Step 3: Try direct city name match
    city = _extract_city_from_text(text)
    if city:
        return city

    # Step 4: Fuzzy matching
    city = fuzzy_match_city(text)
    if city:
        return city

    logger.debug("Could not resolve city", location_text=location_text)
    return None


def _resolve_dublin_postal(text: str) -> str | None:
    """Check for Dublin postal code patterns."""
    # Pattern: Dublin 15, D15, Dublin15
    patterns = [
        r"dublin\s*(\d{1,2}w?)",
        r"d(\d{1,2}w?)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            code = match.group(1).lower()
            return DUBLIN_POSTAL_CODES.get(f"d{code}")

    # Check direct patterns
    text_no_spaces = text.replace(" ", "").replace("-", "")
    for code, name in DUBLIN_POSTAL_CODES.items():
        if code in text_no_spaces:
            return name

    return None


def _resolve_synonym(text: str) -> str | None:
    """Check against known synonyms."""
    text_clean = text.lower().strip()

    # Direct match
    if text_clean in CITY_SYNONYMS:
        return CITY_SYNONYMS[text_clean]

    # Partial match
    for synonym, city_name in CITY_SYNONYMS.items():
        if synonym in text_clean:
            return city_name

    return None


def _extract_city_from_text(text: str) -> dict | None:
    """Try to extract city name from location text."""
    # Split by common separators
    parts = re.split(r"[,|/\-]", text)

    for part in parts:
        part = part.strip()
        if len(part) > 2:
            city = get_city_by_name(part)
            if city:
                return city

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def batch_resolve_cities(location_texts: list[str]) -> list[dict | None]:
    """Resolve multiple locations."""
    return [resolve_city(text) for text in location_texts]


@lru_cache(maxsize=1000)
def cached_resolve_city(location_text: str) -> dict | None:
    """Cached version of resolve_city for performance."""
    return resolve_city(location_text)


def get_resolution_stats() -> dict:
    """Get stats about city resolution cache."""
    info = cached_resolve_city.cache_info()
    return {
        "hits": info.hits,
        "misses": info.misses,
        "size": info.currsize,
        "max_size": info.maxsize,
    }
