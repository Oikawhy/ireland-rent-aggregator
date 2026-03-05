"""
AGPARS Normalization Pipeline

Transform raw scraped data into normalized listing format.
"""

import re
from dataclasses import dataclass
from decimal import Decimal

from packages.observability.logger import get_logger
from packages.observability.metrics import LEASE_LENGTH_UNKNOWN_TOTAL
from packages.storage.listings import get_raw_listing_by_id, upsert_normalized_listing
from packages.storage.models import ListingStatus, PropertyType
from services.collector.runner import RawListing
from services.collector.sanitize import (
    sanitize_beds,
    sanitize_price,
    sanitize_property_type,
)
from services.normalizer.city_synonyms import resolve_city
from services.normalizer.validators import validate_listing
from services.rules.apply_rules import apply_rules

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# NORMALIZATION RESULT
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class NormalizedListing:
    """Normalized listing data."""

    raw_id: int
    price: Decimal | None
    beds: int | None
    baths: int | None
    property_type: PropertyType | None
    furnished: bool | None
    lease_length_months: int | None
    lease_length_unknown: bool
    city_id: int | None
    county: str | None
    area_text: str | None
    status: ListingStatus = ListingStatus.ACTIVE

    # Validation
    is_valid: bool = True
    validation_errors: list[str] | None = None

    # Exclusion result
    is_excluded: bool = False
    exclusion_reason: str | None = None


# ═══════════════════════════════════════════════════════════════════════════════
# NORMALIZATION PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════


class NormalizationPipeline:
    """
    Main normalization pipeline.

    Processes raw listings through:
    1. Field sanitization
    2. City/county resolution
    3. Property type detection
    4. Lease length extraction
    5. Validation
    6. Exclusion rules
    """

    def __init__(self):
        self.logger = get_logger(__name__)

    def normalize(self, raw_listing: RawListing) -> NormalizedListing | None:
        """
        Normalize a raw listing.

        Args:
            raw_listing: Raw listing from scraper

        Returns:
            NormalizedListing or None if excluded
        """
        try:
            # Step 1: Extract and sanitize fields
            price = self._extract_price(raw_listing)
            beds = self._extract_beds(raw_listing)
            baths = self._extract_baths(raw_listing)
            property_type = self._extract_property_type(raw_listing)
            furnished = self._extract_furnished(raw_listing)
            lease_length, lease_unknown = self._extract_lease_length(raw_listing)

            # Step 2: Resolve city/county
            city_id, county, area_text = self._resolve_location(raw_listing)

            # Step 3: Create normalized listing
            normalized = NormalizedListing(
                raw_id=0,  # Will be set during storage
                price=price,
                beds=beds,
                baths=baths,
                property_type=property_type,
                furnished=furnished,
                lease_length_months=lease_length,
                lease_length_unknown=lease_unknown,
                city_id=city_id,
                county=county,
                area_text=area_text,
            )

            if lease_unknown:
                LEASE_LENGTH_UNKNOWN_TOTAL.labels(source=raw_listing.source).inc()

            # Step 4: Validate
            errors = validate_listing(normalized)
            if errors:
                normalized.is_valid = False
                normalized.validation_errors = errors
                self.logger.warning(
                    "Validation errors",
                    source=raw_listing.source,
                    errors=errors,
                )

            # Step 5: Apply exclusion rules
            exclusion = apply_rules(raw_listing, normalized)
            if exclusion:
                normalized.is_excluded = True
                normalized.exclusion_reason = exclusion
                self.logger.info(
                    "Listing excluded",
                    source=raw_listing.source,
                    reason=exclusion,
                )
                return None

            return normalized

        except Exception as e:
            self.logger.error(
                "Normalization failed",
                source=raw_listing.source,
                error=str(e),
            )
            return None

    def _extract_price(self, raw: RawListing) -> Decimal | None:
        """Extract and normalize price."""
        if raw.price_text:
            return sanitize_price(raw.price_text)

        # Try from raw_payload
        payload = raw.raw_payload or {}
        if payload.get("price_text"):
            return sanitize_price(payload["price_text"])

        return None

    def _extract_beds(self, raw: RawListing) -> int | None:
        """Extract bedroom count with fallback from title/description."""
        if raw.beds_text:
            return sanitize_beds(raw.beds_text)

        payload = raw.raw_payload or {}
        if payload.get("beds"):
            return sanitize_beds(str(payload["beds"]))

        # Fallback: parse from title or description
        for text_field in (raw.title, raw.description):
            if text_field:
                match = re.search(r"(\d+)\s*bed", text_field, re.IGNORECASE)
                if match:
                    beds = int(match.group(1))
                    if 0 <= beds <= 20:
                        return beds

        return None

    def _extract_baths(self, raw: RawListing) -> int | None:
        """Extract bathroom count."""
        if raw.baths_text:
            return sanitize_beds(raw.baths_text)  # Same sanitization logic

        payload = raw.raw_payload or {}
        if payload.get("baths"):
            return sanitize_beds(str(payload["baths"]))

        return None

    def _extract_property_type(self, raw: RawListing) -> PropertyType | None:
        """Detect property type."""
        # From explicit field
        if raw.property_type_text:
            return sanitize_property_type(raw.property_type_text)

        # From title
        if raw.title:
            return sanitize_property_type(raw.title)

        return None

    def _extract_furnished(self, raw: RawListing) -> bool | None:
        """Detect furnished status."""
        # Check explicit field
        if raw.furnished_text:
            text = raw.furnished_text.lower()
            if "unfurnished" in text:
                return False
            if "furnished" in text:
                return True

        # Check title/description
        for text in [raw.title, raw.description]:
            if text:
                lower = text.lower()
                if "unfurnished" in lower:
                    return False
                if "furnished" in lower:
                    return True

        return None

    def _extract_lease_length(self, raw: RawListing) -> tuple[int | None, bool]:
        """
        Extract lease length in months.

        Returns:
            Tuple of (months, is_unknown)
        """
        import re

        text = raw.lease_text or raw.description or ""
        if not text:
            return None, True

        text_lower = text.lower()

        # Look for explicit months
        match = re.search(r"(\d+)\s*month", text_lower)
        if match:
            return int(match.group(1)), False

        # Look for year patterns
        match = re.search(r"(\d+)\s*year", text_lower)
        if match:
            return int(match.group(1)) * 12, False

        # Common patterns
        if "minimum 12 months" in text_lower or "min 12 months" in text_lower:
            return 12, False
        if "minimum 6 months" in text_lower or "min 6 months" in text_lower:
            return 6, False
        if "long term" in text_lower or "long-term" in text_lower:
            return 12, False  # Assume 12 months for long term

        return None, True

    def _resolve_location(self, raw: RawListing) -> tuple[int | None, str | None, str | None]:
        """
        Resolve city ID and county from location text.

        Returns:
            Tuple of (city_id, county, area_text)
        """
        location_text = raw.location_text or ""

        # Try to resolve city
        city_result = resolve_city(location_text)

        if city_result:
            return city_result["id"], city_result["county"], location_text

        # County fallback: extract county from location text when no city match
        county = self._extract_county_fallback(location_text)
        return None, county, location_text

    def _extract_county_fallback(self, location_text: str) -> str | None:
        """
        Extract county name from location text when city resolution fails.

        Returns:
            County name or None
        """
        import re

        if not location_text:
            return None

        text_lower = location_text.lower()

        # Irish counties (Republic of Ireland only - no NI)
        irish_counties = [
            "Dublin", "Cork", "Galway", "Limerick", "Waterford",
            "Kildare", "Meath", "Wicklow", "Wexford", "Kerry",
            "Tipperary", "Clare", "Donegal", "Mayo", "Roscommon",
            "Sligo", "Leitrim", "Cavan", "Monaghan", "Louth",
            "Westmeath", "Offaly", "Laois", "Kilkenny", "Carlow",
            "Longford",
        ]

        # Check for "Co. Dublin", "County Dublin", "Dublin County"
        for county in irish_counties:
            patterns = [
                rf"\bco\.?\s*{county.lower()}\b",
                rf"\bcounty\s+{county.lower()}\b",
                rf"\b{county.lower()}\s+county\b",
                rf"\b{county.lower()}\b",  # Simple match
            ]
            for pattern in patterns:
                if re.search(pattern, text_lower):
                    return county

        return None


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════


def process_raw_listing(raw_id: int) -> NormalizedListing | None:
    """
    Process a raw listing by ID.

    Fetches from database, normalizes, and saves.
    """
    raw_data = get_raw_listing_by_id(raw_id)
    if not raw_data:
        return None

    # Reconstruct RawListing object
    raw = RawListing(
        source=raw_data["source"],
        source_listing_id=raw_data["source_listing_id"],
        url=raw_data["url"],
        raw_payload=raw_data.get("raw_payload", {}),
        first_photo_url=raw_data.get("first_photo_url"),
    )

    # Populate fields: prefer DB columns, fall back to raw_payload
    payload = raw_data.get("raw_payload", {})
    raw.title = raw_data.get("title") or payload.get("title")
    raw.price_text = raw_data.get("price_text") or payload.get("price_text")
    raw.location_text = raw_data.get("location_text") or payload.get("location_text")
    raw.beds_text = raw_data.get("beds_text") or payload.get("beds_text")
    raw.baths_text = raw_data.get("baths_text") or payload.get("baths_text")
    raw.property_type_text = raw_data.get("property_type_text") or payload.get("property_type_text")
    raw.description = raw_data.get("description") or payload.get("description")

    # Normalize
    pipeline = NormalizationPipeline()
    normalized = pipeline.normalize(raw)

    if normalized:
        normalized.raw_id = raw_id

        # Save to database
        upsert_normalized_listing(
            raw_id=raw_id,
            price=normalized.price,
            beds=normalized.beds,
            baths=normalized.baths,
            property_type=normalized.property_type,
            furnished=normalized.furnished,
            lease_length_months=normalized.lease_length_months,
            lease_length_unknown=normalized.lease_length_unknown,
            city_id=normalized.city_id,
            county=normalized.county,
            area_text=normalized.area_text,
            status=normalized.status,
        )

    return normalized


def batch_normalize(raw_ids: list[int]) -> dict:
    """
    Normalize a batch of raw listings.

    Returns:
        Stats dict with success/failure/excluded counts
    """
    stats = {"success": 0, "failed": 0, "excluded": 0}

    for raw_id in raw_ids:
        result = process_raw_listing(raw_id)
        if result:
            stats["success"] += 1
        elif result is None:
            stats["excluded"] += 1
        else:
            stats["failed"] += 1

    logger.info("Batch normalization complete", **stats)
    return stats
