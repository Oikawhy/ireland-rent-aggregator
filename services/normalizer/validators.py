"""
AGPARS Data Validators

Validation rules for normalized listing data.
"""

import re
from typing import Any

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION RULES
# ═══════════════════════════════════════════════════════════════════════════════


# Price ranges (EUR per month)
MIN_PRICE = 100
MAX_PRICE = 50000

# Bedroom ranges
MIN_BEDS = 0  # Studio
MAX_BEDS = 20

# Bathroom ranges
MIN_BATHS = 0
MAX_BATHS = 10

# URL patterns
URL_PATTERN = re.compile(r"^https?://")


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATORS
# ═══════════════════════════════════════════════════════════════════════════════


def validate_listing(listing: Any) -> list[str]:
    """
    Validate a normalized listing.

    Args:
        listing: NormalizedListing or dict

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    # Convert to dict if needed
    if hasattr(listing, "__dict__"):
        data = {
            "price": getattr(listing, "price", None),
            "beds": getattr(listing, "beds", None),
            "baths": getattr(listing, "baths", None),
            "property_type": getattr(listing, "property_type", None),
            "city_id": getattr(listing, "city_id", None),
            "county": getattr(listing, "county", None),
            "area_text": getattr(listing, "area_text", None),
        }
    else:
        data = listing

    # Validate price
    price_errors = validate_price(data.get("price"))
    errors.extend(price_errors)

    # Validate beds
    beds_errors = validate_beds(data.get("beds"))
    errors.extend(beds_errors)

    # Validate baths
    baths_errors = validate_baths(data.get("baths"))
    errors.extend(baths_errors)

    # Validate location
    location_errors = validate_location(data)
    errors.extend(location_errors)

    return errors


def validate_price(price: Any) -> list[str]:
    """Validate price value."""
    errors = []

    if price is None:
        # Price is optional but recommended
        return []

    try:
        price_float = float(price)
    except (TypeError, ValueError):
        errors.append(f"Invalid price format: {price}")
        return errors

    if price_float < MIN_PRICE:
        errors.append(f"Price too low: {price_float} (min: {MIN_PRICE})")

    if price_float > MAX_PRICE:
        errors.append(f"Price too high: {price_float} (max: {MAX_PRICE})")

    return errors


def validate_beds(beds: Any) -> list[str]:
    """Validate bedroom count."""
    errors = []

    if beds is None:
        return []

    try:
        beds_int = int(beds)
    except (TypeError, ValueError):
        errors.append(f"Invalid beds format: {beds}")
        return errors

    if beds_int < MIN_BEDS:
        errors.append(f"Invalid beds count: {beds_int}")

    if beds_int > MAX_BEDS:
        errors.append(f"Beds count too high: {beds_int} (max: {MAX_BEDS})")

    return errors


def validate_baths(baths: Any) -> list[str]:
    """Validate bathroom count."""
    errors = []

    if baths is None:
        return []

    try:
        baths_int = int(baths)
    except (TypeError, ValueError):
        errors.append(f"Invalid baths format: {baths}")
        return errors

    if baths_int < MIN_BATHS:
        errors.append(f"Invalid baths count: {baths_int}")

    if baths_int > MAX_BATHS:
        errors.append(f"Baths count too high: {baths_int} (max: {MAX_BATHS})")

    return errors


def validate_location(data: dict) -> list[str]:
    """Validate location data."""
    errors = []

    city_id = data.get("city_id")
    county = data.get("county")
    area_text = data.get("area_text")

    # At least one location identifier should be present
    if city_id is None and county is None and not area_text:
        errors.append("No location information available")

    return errors


def validate_url(url: str | None) -> list[str]:
    """Validate URL format."""
    errors = []

    if not url:
        errors.append("URL is required")
        return errors

    if not URL_PATTERN.match(url):
        errors.append(f"Invalid URL format: {url}")

    # Check for common issues
    if " " in url:
        errors.append("URL contains spaces")

    if len(url) > 2048:
        errors.append("URL too long")

    return errors


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def is_valid_listing(listing: Any) -> bool:
    """Check if listing is valid."""
    return len(validate_listing(listing)) == 0


def get_validation_summary(listings: list[Any]) -> dict:
    """Get validation summary for a batch of listings."""
    valid_count = 0
    invalid_count = 0
    error_counts: dict[str, int] = {}

    for listing in listings:
        errors = validate_listing(listing)
        if errors:
            invalid_count += 1
            for error in errors:
                # Extract error type
                error_type = error.split(":")[0]
                error_counts[error_type] = error_counts.get(error_type, 0) + 1
        else:
            valid_count += 1

    return {
        "total": len(listings),
        "valid": valid_count,
        "invalid": invalid_count,
        "error_breakdown": error_counts,
    }
