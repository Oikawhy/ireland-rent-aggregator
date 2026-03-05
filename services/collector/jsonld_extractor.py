"""
AGPARS JSON-LD Extractor Module

Extract structured data from JSON-LD / Schema.org markup.
"""

import json
import re
from typing import Any

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# JSON-LD EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════


def extract_jsonld(html: str) -> list[dict]:
    """
    Extract all JSON-LD blocks from HTML.

    Args:
        html: Raw HTML content

    Returns:
        List of parsed JSON-LD objects
    """
    results = []

    # Find all JSON-LD script tags
    pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
    matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)

    for match in matches:
        try:
            # Clean up the JSON
            cleaned = match.strip()
            if cleaned:
                data = json.loads(cleaned)

                # Handle both single object and array
                if isinstance(data, list):
                    results.extend(data)
                else:
                    results.append(data)

        except json.JSONDecodeError as e:
            logger.debug("Failed to parse JSON-LD", error=str(e))
            continue

    return results


def find_listing_data(jsonld_items: list[dict]) -> dict | None:
    """
    Find real estate listing data in JSON-LD items.

    Looks for Schema.org types:
    - RealEstateListing
    - Residence
    - Apartment
    - House
    - Product (sometimes used for listings)

    Returns:
        Best matching listing data or None
    """
    listing_types = {
        "RealEstateListing",
        "Residence",
        "Apartment",
        "House",
        "SingleFamilyResidence",
        "ApartmentComplex",
        "Product",
        "Offer",
    }

    for item in jsonld_items:
        item_type = item.get("@type", "")

        # Handle array types
        types = set(item_type) if isinstance(item_type, list) else {item_type}

        if types & listing_types:
            return item

        # Check nested @graph
        if "@graph" in item:
            nested = find_listing_data(item["@graph"])
            if nested:
                return nested

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# DATA MAPPING
# ═══════════════════════════════════════════════════════════════════════════════


def map_jsonld_to_listing(data: dict) -> dict:
    """
    Map JSON-LD Schema.org data to internal listing format.

    Args:
        data: JSON-LD object

    Returns:
        Dict with extracted listing fields
    """
    result = {
        "title": None,
        "price": None,
        "currency": None,
        "beds": None,
        "baths": None,
        "property_type": None,
        "url": None,
        "description": None,
        "address": None,
        "city": None,
        "county": None,
        "images": [],
    }

    # Title / Name
    result["title"] = data.get("name") or data.get("headline")

    # Description
    result["description"] = data.get("description")

    # URL
    result["url"] = data.get("url")

    # Price from offers
    offers = data.get("offers") or data.get("priceSpecification")
    if offers:
        if isinstance(offers, list):
            offers = offers[0]
        result["price"] = offers.get("price") or offers.get("priceRange")
        result["currency"] = offers.get("priceCurrency", "EUR")

    # Direct price
    if not result["price"]:
        result["price"] = data.get("price")

    # Address
    address = data.get("address")
    if address:
        if isinstance(address, str):
            result["address"] = address
        elif isinstance(address, dict):
            parts = []
            if address.get("streetAddress"):
                parts.append(address["streetAddress"])
            if address.get("addressLocality"):
                result["city"] = address["addressLocality"]
                parts.append(address["addressLocality"])
            if address.get("addressRegion"):
                result["county"] = address["addressRegion"]
                parts.append(address["addressRegion"])
            result["address"] = ", ".join(parts) if parts else None

    # Property type
    item_type = data.get("@type", "")
    if isinstance(item_type, list):
        item_type = item_type[0] if item_type else ""

    type_mapping = {
        "Apartment": "apartment",
        "House": "house",
        "SingleFamilyResidence": "house",
        "ApartmentComplex": "apartment",
        "Residence": "other",
    }
    result["property_type"] = type_mapping.get(item_type)

    # Bedrooms/Bathrooms
    result["beds"] = _extract_number(data.get("numberOfBedrooms") or data.get("numberOfRooms"))
    result["baths"] = _extract_number(data.get("numberOfBathroomsTotal"))

    # Images
    images = data.get("image") or data.get("photo")
    if images:
        if isinstance(images, str):
            result["images"] = [images]
        elif isinstance(images, list):
            result["images"] = [
                img if isinstance(img, str) else img.get("url", img.get("contentUrl"))
                for img in images
                if img
            ]

    # Floor size (might be useful)
    floor_size = data.get("floorSize")
    if floor_size and isinstance(floor_size, dict):
        result["floor_size"] = floor_size.get("value")
        result["floor_size_unit"] = floor_size.get("unitCode", "SQM")

    return result


def _extract_number(value: Any) -> int | None:
    """Extract integer from various formats."""
    if value is None:
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        match = re.search(r"(\d+)", value)
        if match:
            return int(match.group(1))

    if isinstance(value, dict):
        return _extract_number(value.get("value"))

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def extract_listing_from_html(html: str) -> dict | None:
    """
    Extract listing data from HTML using JSON-LD.

    Args:
        html: Raw HTML content

    Returns:
        Mapped listing data or None if not found
    """
    jsonld_items = extract_jsonld(html)
    if not jsonld_items:
        return None

    listing_data = find_listing_data(jsonld_items)
    if not listing_data:
        return None

    return map_jsonld_to_listing(listing_data)


async def extract_listing_from_page(page: Any) -> dict | None:
    """
    Extract listing data from a Playwright page.

    Args:
        page: Playwright Page instance

    Returns:
        Mapped listing data or None
    """
    try:
        html = await page.content()
        return extract_listing_from_html(html)
    except Exception as e:
        logger.error("Failed to extract JSON-LD from page", error=str(e))
        return None
