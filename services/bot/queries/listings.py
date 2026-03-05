"""
AGPARS Listing Queries

Read from pub.public_listings with filter support.

Covers T065.8.
"""

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session

logger = get_logger(__name__)


def get_latest_listings(
    filters: dict | None = None,
    limit: int = 10,
    offset: int = 0,
    exclude_ids: list[int] | None = None,
) -> list[dict]:
    """
    Get latest listings from pub.public_listings.

    Args:
        filters: Subscription filter dict (budget, beds, counties, etc.)
        limit: Max results
        offset: Pagination offset
        exclude_ids: Listing IDs to exclude (hidden listings)

    Returns:
        List of listing dicts
    """
    conditions = ["status = 'active'"]
    params: dict = {"limit": limit, "offset": offset}

    if filters:
        conditions, params = _apply_filters(conditions, params, filters)

    if exclude_ids:
        conditions.append("listing_id != ALL(:exclude_ids)")
        params["exclude_ids"] = exclude_ids

    where_clause = " AND ".join(conditions)

    query = f"""
        WITH deduped AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY COALESCE(cluster_id, listing_id)
                ORDER BY published_at DESC
            ) AS rn
            FROM pub.public_listings
            WHERE {where_clause}
        )
        SELECT listing_id, raw_id, source, url, price, beds, baths,
               property_type, county, city, area_text, first_photo_url,
               published_at, updated_at
        FROM deduped
        WHERE rn = 1
        ORDER BY published_at DESC
        LIMIT :limit OFFSET :offset
    """

    with get_readonly_session() as session:
        result = session.execute(text(query), params)
        return [dict(row._mapping) for row in result.fetchall()]


def get_listing_count(
    filters: dict | None = None,
    exclude_ids: list[int] | None = None,
) -> int:
    """Get total count of listings matching filters."""
    conditions = ["status = 'active'"]
    params: dict = {}

    if filters:
        conditions, params = _apply_filters(conditions, params, filters)

    if exclude_ids:
        conditions.append("listing_id != ALL(:exclude_ids)")
        params["exclude_ids"] = exclude_ids

    where_clause = " AND ".join(conditions)

    query = f"""
        WITH deduped AS (
            SELECT listing_id, cluster_id, ROW_NUMBER() OVER (
                PARTITION BY COALESCE(cluster_id, listing_id)
                ORDER BY published_at DESC
            ) AS rn
            FROM pub.public_listings
            WHERE {where_clause}
        )
        SELECT COUNT(*) FROM deduped WHERE rn = 1
    """

    with get_readonly_session() as session:
        result = session.execute(text(query), params)
        return result.scalar() or 0


def get_listing_by_id(listing_id: int) -> dict | None:
    """Get a single listing by ID."""
    query = """
        SELECT listing_id, raw_id, source, url, price, beds, baths,
               property_type, county, city, area_text, first_photo_url,
               published_at, updated_at
        FROM pub.public_listings
        WHERE listing_id = :listing_id
    """
    with get_readonly_session() as session:
        result = session.execute(text(query), {"listing_id": listing_id})
        row = result.fetchone()
        return dict(row._mapping) if row else None


def _apply_filters(
    conditions: list[str],
    params: dict,
    filters: dict,
) -> tuple[list[str], dict]:
    """Apply subscription filters to query conditions."""

    if filters.get("min_budget") is not None:
        conditions.append("price >= :min_budget")
        params["min_budget"] = float(filters["min_budget"])

    if filters.get("max_budget") is not None:
        conditions.append("price <= :max_budget")
        params["max_budget"] = float(filters["max_budget"])

    if filters.get("min_beds") is not None:
        conditions.append("beds >= :min_beds")
        params["min_beds"] = int(filters["min_beds"])

    if filters.get("max_beds") is not None:
        conditions.append("beds <= :max_beds")
        params["max_beds"] = int(filters["max_beds"])

    if filters.get("counties"):
        # Case-insensitive county match
        conditions.append("LOWER(county) = ANY(:counties)")
        params["counties"] = [c.lower() for c in filters["counties"]]

    if filters.get("property_types"):
        conditions.append("LOWER(property_type) = ANY(:property_types)")
        params["property_types"] = [t.lower() for t in filters["property_types"]]

    if filters.get("cities"):
        conditions.append("LOWER(city) = ANY(:cities)")
        params["cities"] = [c.lower() for c in filters["cities"]]

    return conditions, params
