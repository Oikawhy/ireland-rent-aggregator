"""
AGPARS City Stats Query

Reads pub.city_stats materialized view for bot city filter.
"""

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session

logger = get_logger(__name__)


def get_cities_for_county(county: str) -> list[dict]:
    """
    Get cities with listing counts for a given county.

    Reads from pub.city_stats materialized view.

    Args:
        county: County name (e.g. "Dublin")

    Returns:
        List of dicts: [{"city": "Rathmines", "count": 12}, ...]
        Sorted by count descending.
    """
    with get_readonly_session() as session:
        result = session.execute(
            text("""
                SELECT city, listing_count
                FROM pub.city_stats
                WHERE county = :county
                ORDER BY listing_count DESC, city ASC
            """),
            {"county": county},
        )
        rows = result.fetchall()

    cities = [{"city": r.city, "count": r.listing_count} for r in rows]
    logger.debug("Cities loaded for county", county=county, count=len(cities))
    return cities
