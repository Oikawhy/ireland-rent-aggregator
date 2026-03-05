"""
AGPARS City Synonyms Module

City synonym mapping for flexible matching.
"""


from packages.observability.logger import get_logger
from packages.storage.cities import get_all_cities

logger = get_logger(__name__)


# Common city name variations (source-specific)
HARDCODED_SYNONYMS = {
    "dublin": ["dublin city", "dublin 1", "dublin 2", "dublin 4", "dublin 6", "dublin 8"],
    "cork": ["cork city"],
    "galway": ["galway city"],
    "limerick": ["limerick city"],
    "waterford": ["waterford city"],
    "dun laoghaire": ["dún laoghaire", "dunlaoghaire"],
    "bray": ["bray, co wicklow"],
    "drogheda": ["drogheda, co louth"],
}


def get_synonym_map() -> dict[str, str]:
    """
    Build a map of synonyms to canonical city names.

    Returns:
        Dict mapping synonym -> canonical name
    """
    synonym_map = {}

    # Load from database
    for city in get_all_cities():
        canonical = city["name"].lower()

        # Map name to itself
        synonym_map[canonical] = city["name"]

        # Map database synonyms
        for syn in city.get("synonyms", []):
            synonym_map[syn.lower()] = city["name"]

    # Add hardcoded synonyms
    for canonical, synonyms in HARDCODED_SYNONYMS.items():
        # Find the proper-cased canonical name
        proper_name = next(
            (c["name"] for c in get_all_cities() if c["name"].lower() == canonical),
            canonical.title(),
        )
        for syn in synonyms:
            synonym_map[syn.lower()] = proper_name

    return synonym_map


def resolve_city_name(input_name: str) -> str | None:
    """
    Resolve a city name input to its canonical form.

    Args:
        input_name: Raw city name from user input or scraping

    Returns:
        Canonical city name or None if not found
    """
    if not input_name:
        return None

    cleaned = input_name.lower().strip()

    # Remove common prefixes/suffixes
    prefixes = ["co. ", "co ", "county "]
    for prefix in prefixes:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):]

    suffixes = [", ireland", ", co dublin", ", co cork", ", co galway"]
    for suffix in suffixes:
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)]

    synonym_map = get_synonym_map()
    return synonym_map.get(cleaned)


def add_synonym(city_name: str, synonym: str) -> bool:
    """
    Add a synonym for a city (updates database).

    Returns:
        True if added successfully
    """
    from packages.storage.db import get_session
    from packages.storage.models import City

    with get_session() as session:
        city = session.query(City).filter(City.name == city_name).first()
        if not city:
            logger.warning("City not found", city=city_name)
            return False

        synonyms = list(city.synonyms or [])
        if synonym.lower() not in [s.lower() for s in synonyms]:
            synonyms.append(synonym)
            city.synonyms = synonyms
            logger.info("Synonym added", city=city_name, synonym=synonym)
            return True
        return False


def get_all_synonyms(city_name: str) -> list[str]:
    """Get all synonyms for a city."""
    for city in get_all_cities():
        if city["name"].lower() == city_name.lower():
            return list(city.get("synonyms", []))
    return []
