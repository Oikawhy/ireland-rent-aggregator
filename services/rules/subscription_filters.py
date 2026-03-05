"""
AGPARS Subscription Filters

Match listings to subscriptions based on filter criteria.
"""


from packages.observability.logger import get_logger
from packages.storage.subscriptions import get_active_subscriptions

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# FILTER MATCHING
# ═══════════════════════════════════════════════════════════════════════════════


def match_listing_to_subscriptions(listing: dict) -> list[int]:
    """
    Find all subscriptions that match a listing.

    Args:
        listing: Normalized listing dict

    Returns:
        List of matching subscription IDs
    """
    subscriptions = get_active_subscriptions()
    matching_ids = []

    for sub in subscriptions:
        if matches_subscription(listing, sub):
            matching_ids.append(sub["id"])

    return matching_ids


def matches_subscription(listing: dict, subscription: dict) -> bool:
    """
    Check if a listing matches a subscription's filters.

    Args:
        listing: Normalized listing dict
        subscription: Subscription dict with filters

    Returns:
        True if listing matches all filters
    """
    filters = subscription.get("filters", {})

    if not filters:
        # No filters = match everything
        return True

    # Check each filter
    if not _matches_budget(listing, filters):
        return False

    if not _matches_bedrooms(listing, filters):
        return False

    if not _matches_property_type(listing, filters):
        return False

    if not _matches_furnished(listing, filters):
        return False

    return _matches_location(listing, filters)


# ═══════════════════════════════════════════════════════════════════════════════
# INDIVIDUAL FILTER CHECKS
# ═══════════════════════════════════════════════════════════════════════════════


def _matches_budget(listing: dict, filters: dict) -> bool:
    """Check if listing matches budget filter."""
    min_budget = filters.get("min_budget")
    max_budget = filters.get("max_budget")

    if min_budget is None and max_budget is None:
        return True

    price = listing.get("price")
    if price is None:
        # No price = can't match budget filter
        return False

    try:
        price_float = float(price)
    except (TypeError, ValueError):
        return False

    if min_budget is not None and price_float < float(min_budget):
        return False

    return not (max_budget is not None and price_float > float(max_budget))


def _matches_bedrooms(listing: dict, filters: dict) -> bool:
    """Check if listing matches bedroom filter."""
    min_beds = filters.get("min_beds")
    max_beds = filters.get("max_beds")

    if min_beds is None and max_beds is None:
        return True

    beds = listing.get("beds")
    if beds is None:
        # No beds info = include if filter is permissive
        return min_beds is None or min_beds == 0

    try:
        beds_int = int(beds)
    except (TypeError, ValueError):
        return False

    if min_beds is not None and beds_int < int(min_beds):
        return False

    return not (max_beds is not None and beds_int > int(max_beds))


def _matches_property_type(listing: dict, filters: dict) -> bool:
    """Check if listing matches property type filter."""
    allowed_types = filters.get("property_types")

    if not allowed_types:
        return True

    listing_type = listing.get("property_type")
    if listing_type is None:
        # Unknown type = include by default
        return True

    # Normalize for comparison
    if hasattr(listing_type, "value"):
        listing_type = listing_type.value

    return listing_type.lower() in [t.lower() for t in allowed_types]


def _matches_furnished(listing: dict, filters: dict) -> bool:
    """Check if listing matches furnished filter."""
    required_furnished = filters.get("furnished")

    if required_furnished is None:
        return True

    listing_furnished = listing.get("furnished")

    # If listing doesn't have furnished info, include it
    if listing_furnished is None:
        return True

    return listing_furnished == required_furnished


def _matches_location(listing: dict, filters: dict) -> bool:
    """Check if listing matches location filters."""
    # Check city IDs
    city_ids = filters.get("city_ids", [])
    if city_ids:
        listing_city_id = listing.get("city_id")
        if listing_city_id is None:
            return False
        if listing_city_id not in city_ids:
            return False

    # Check counties
    counties = filters.get("counties", [])
    if counties:
        listing_county = listing.get("county")
        if listing_county is None:
            return False
        # Case-insensitive match
        if listing_county.lower() not in [c.lower() for c in counties]:
            return False

    return True


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def filter_listings_for_subscription(
    listings: list[dict],
    subscription: dict,
) -> list[dict]:
    """
    Filter a list of listings for a specific subscription.

    Args:
        listings: List of normalized listings
        subscription: Subscription dict with filters

    Returns:
        List of matching listings
    """
    return [listing for listing in listings if matches_subscription(listing, subscription)]


def get_matching_subscriptions_for_listing(listing: dict) -> list[dict]:
    """
    Get full subscription details for all matching subscriptions.

    Args:
        listing: Normalized listing dict

    Returns:
        List of matching subscription dicts
    """
    subscriptions = get_active_subscriptions()
    return [sub for sub in subscriptions if matches_subscription(listing, sub)]


def get_subscription_match_stats(listings: list[dict]) -> dict:
    """
    Get statistics about subscription matching.

    Args:
        listings: List of listings

    Returns:
        Dict with stats per subscription
    """
    subscriptions = get_active_subscriptions()
    stats = {}

    for sub in subscriptions:
        sub_id = sub["id"]
        matches = filter_listings_for_subscription(listings, sub)
        stats[sub_id] = {
            "subscription_id": sub_id,
            "name": sub.get("name"),
            "workspace_id": sub.get("workspace_id"),
            "total_matches": len(matches),
        }

    return stats
