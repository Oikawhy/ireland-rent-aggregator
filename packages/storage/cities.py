"""
AGPARS City Storage Module

City master list retrieval and caching.
"""

from functools import lru_cache

from sqlalchemy import select

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session
from packages.storage.models import City

logger = get_logger(__name__)


@lru_cache(maxsize=1)
def get_all_cities() -> list[dict]:
    """
    Get all cities from the master list.

    Returns:
        List of city dicts with id, name, county, population
    """
    with get_readonly_session() as session:
        result = session.execute(select(City))
        cities = [
            {
                "id": c.id,
                "name": c.name,
                "county": c.county,
                "population": c.population,
                "synonyms": c.synonyms or [],
            }
            for c in result.scalars().all()
        ]
        logger.info("Cities loaded", count=len(cities))
        return cities


def get_city_by_name(name: str) -> dict | None:
    """Get a city by exact name match."""
    cities = get_all_cities()
    name_lower = name.lower().strip()
    for city in cities:
        if city["name"].lower() == name_lower:
            return city
    return None


def get_cities_by_county(county: str) -> list[dict]:
    """Get all cities in a county."""
    cities = get_all_cities()
    county_lower = county.lower().strip()
    return [c for c in cities if c["county"].lower() == county_lower]


def get_city_names() -> list[str]:
    """Get list of all city names."""
    return [c["name"] for c in get_all_cities()]


def get_active_cities() -> list[dict]:
    """
    Get cities that have active subscriptions.

    For now, returns all cities. Will be optimized when subscriptions are active.
    """
    # TODO: Filter by cities in active subscriptions
    return get_all_cities()


def fuzzy_match_city(input_name: str, threshold: float = 0.8) -> dict | None:
    """
    Find a city using fuzzy matching.

    Args:
        input_name: User input city name
        threshold: Minimum similarity score (0-1)

    Returns:
        Best matching city or None
    """
    from difflib import SequenceMatcher

    cities = get_all_cities()
    input_lower = input_name.lower().strip()

    best_match = None
    best_score = 0

    for city in cities:
        # Check exact match first
        if city["name"].lower() == input_lower:
            return city

        # Check synonyms
        for synonym in city.get("synonyms", []):
            if synonym.lower() == input_lower:
                return city

        # Calculate similarity
        score = SequenceMatcher(None, input_lower, city["name"].lower()).ratio()
        if score > best_score and score >= threshold:
            best_score = score
            best_match = city

    return best_match


def clear_city_cache() -> None:
    """Clear the city cache (call after imports)."""
    get_all_cities.cache_clear()
    logger.info("City cache cleared")


def get_city_count() -> int:
    """Get total number of cities."""
    return len(get_all_cities())
