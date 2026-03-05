"""
AGPARS Query Filters

Filter building utilities for listing queries.

Covers T065.9.
"""

from packages.observability.logger import get_logger

logger = get_logger(__name__)


def build_filter_dict(
    min_budget: int | None = None,
    max_budget: int | None = None,
    min_beds: int | None = None,
    max_beds: int | None = None,
    counties: list[str] | None = None,
    city_ids: list[int] | None = None,
    property_types: list[str] | None = None,
    sources: list[str] | None = None,
) -> dict:
    """
    Build a normalized filter dict from individual parameters.

    Returns:
        Filter dict suitable for subscription storage and query application
    """
    filters = {}

    if min_budget is not None:
        filters["min_budget"] = min_budget
    if max_budget is not None:
        filters["max_budget"] = max_budget
    if min_beds is not None:
        filters["min_beds"] = min_beds
    if max_beds is not None:
        filters["max_beds"] = max_beds
    if counties:
        filters["counties"] = [c.title() for c in counties]
    if city_ids:
        filters["city_ids"] = city_ids
    if property_types:
        filters["property_types"] = [t.lower() for t in property_types]
    if sources:
        filters["sources"] = [s.lower() for s in sources]

    return filters


def merge_filters(existing: dict, new: dict) -> dict:
    """
    Merge new filter values into existing filters.

    New values override existing ones. Empty lists clear the filter.

    Returns:
        Merged filter dict
    """
    merged = dict(existing)

    for key, value in new.items():
        if isinstance(value, list) and not value:
            # Empty list = clear this filter
            merged.pop(key, None)
        else:
            merged[key] = value

    return merged


def describe_filters(filters: dict) -> str:
    """
    Generate a human-readable description of active filters.

    Returns:
        Description string for display
    """
    if not filters:
        return "No filters (all listings)"

    parts = []

    min_b = filters.get("min_budget")
    max_b = filters.get("max_budget")
    if min_b and max_b:
        parts.append(f"💰 €{min_b:,}–€{max_b:,}")
    elif min_b:
        parts.append(f"💰 From €{min_b:,}")
    elif max_b:
        parts.append(f"💰 Up to €{max_b:,}")

    min_beds = filters.get("min_beds")
    max_beds = filters.get("max_beds")
    if min_beds and max_beds and min_beds == max_beds:
        parts.append(f"🛏 {min_beds} beds")
    elif min_beds:
        parts.append(f"🛏 {min_beds}+ beds")
    elif max_beds:
        parts.append(f"🛏 Up to {max_beds} beds")

    if filters.get("counties"):
        parts.append(f"📍 {', '.join(filters['counties'])}")

    if filters.get("cities"):
        parts.append(f"🏙 {', '.join(filters['cities'])}")

    if filters.get("property_types"):
        parts.append(f"🏠 {', '.join(filters['property_types'])}")

    return " | ".join(parts) if parts else "All listings"
