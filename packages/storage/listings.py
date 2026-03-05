"""
AGPARS Listings Storage Module

CRUD operations for raw and normalized listings.
"""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select, update

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session, get_session
from packages.storage.models import (
    ListingNormalized,
    ListingRaw,
    ListingStatus,
    PropertyType,
)

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# RAW LISTINGS
# ═══════════════════════════════════════════════════════════════════════════════


def upsert_raw_listing(
    source: str,
    source_listing_id: str,
    url: str,
    raw_payload: dict | None = None,
    first_photo_url: str | None = None,
    title: str | None = None,
    price_text: str | None = None,
    beds_text: str | None = None,
    baths_text: str | None = None,
    property_type_text: str | None = None,
    location_text: str | None = None,
    description: str | None = None,
) -> tuple[int, bool]:
    """
    Insert or update a raw listing.

    Returns:
        Tuple of (raw_id, is_new) where is_new indicates if this is a new listing
    """
    with get_session() as session:
        # Try to find existing
        existing = _get_raw_listing_by_source_id(session, source, source_listing_id)

        if existing:
            # Update last_seen, payload and parsed fields
            stmt = (
                update(ListingRaw)
                .where(ListingRaw.id == existing.id)
                .values(
                    raw_payload=raw_payload,
                    first_photo_url=first_photo_url,
                    last_seen=datetime.utcnow(),
                    title=title,
                    price_text=price_text,
                    beds_text=beds_text,
                    baths_text=baths_text,
                    property_type_text=property_type_text,
                    location_text=location_text,
                    description=description,
                )
            )
            session.execute(stmt)
            logger.debug("Raw listing updated", raw_id=existing.id, source=source)
            return existing.id, False
        else:
            # Insert new
            listing = ListingRaw(
                source=source,
                source_listing_id=source_listing_id,
                url=url,
                raw_payload=raw_payload,
                first_photo_url=first_photo_url,
                title=title,
                price_text=price_text,
                beds_text=beds_text,
                baths_text=baths_text,
                property_type_text=property_type_text,
                location_text=location_text,
                description=description,
            )
            session.add(listing)
            session.flush()
            logger.info("Raw listing created", raw_id=listing.id, source=source)
            return listing.id, True


def get_raw_listing_by_source_id(source: str, source_listing_id: str) -> dict | None:
    """Get raw listing by source and source listing ID."""
    with get_readonly_session() as session:
        listing = _get_raw_listing_by_source_id(session, source, source_listing_id)
        if listing:
            return _raw_to_dict(listing)
        return None


def get_raw_listing_by_id(raw_id: int) -> dict | None:
    """Get raw listing by ID."""
    with get_readonly_session() as session:
        listing = session.get(ListingRaw, raw_id)
        if listing:
            return _raw_to_dict(listing)
        return None


def _get_raw_listing_by_source_id(session, source: str, source_listing_id: str) -> ListingRaw | None:
    """Internal: get raw listing within session."""
    query = (
        select(ListingRaw)
        .where(ListingRaw.source == source)
        .where(ListingRaw.source_listing_id == source_listing_id)
    )
    result = session.execute(query)
    return result.scalar_one_or_none()


def _raw_to_dict(listing: ListingRaw) -> dict:
    """Convert ListingRaw to dictionary."""
    return {
        "id": listing.id,
        "source": listing.source,
        "source_listing_id": listing.source_listing_id,
        "url": listing.url,
        "raw_payload": listing.raw_payload,
        "first_photo_url": listing.first_photo_url,
        "first_seen": listing.first_seen,
        "last_seen": listing.last_seen,
        # Parsed text fields (extracted by adapters)
        "title": listing.title,
        "price_text": listing.price_text,
        "beds_text": listing.beds_text,
        "baths_text": listing.baths_text,
        "property_type_text": listing.property_type_text,
        "location_text": listing.location_text,
        "description": listing.description,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# NORMALIZED LISTINGS
# ═══════════════════════════════════════════════════════════════════════════════


def upsert_normalized_listing(
    raw_id: int,
    price: Decimal | None = None,
    beds: int | None = None,
    baths: int | None = None,
    property_type: PropertyType | None = None,
    furnished: bool | None = None,
    lease_length_months: int | None = None,
    lease_length_unknown: bool = False,
    city_id: int | None = None,
    county: str | None = None,
    area_text: str | None = None,
    status: ListingStatus = ListingStatus.ACTIVE,
) -> tuple[int, bool]:
    """
    Insert or update a normalized listing.

    Returns:
        Tuple of (normalized_id, is_new)
    """
    with get_session() as session:
        existing = _get_normalized_by_raw_id(session, raw_id)

        if existing:
            # Update
            stmt = (
                update(ListingNormalized)
                .where(ListingNormalized.raw_id == raw_id)
                .values(
                    price=price,
                    beds=beds,
                    baths=baths,
                    property_type=property_type,
                    furnished=furnished,
                    lease_length_months=lease_length_months,
                    lease_length_unknown=lease_length_unknown,
                    city_id=city_id,
                    county=county,
                    area_text=area_text,
                    status=status,
                    updated_at=datetime.utcnow(),
                )
            )
            session.execute(stmt)
            logger.debug("Normalized listing updated", raw_id=raw_id)
            return existing.id, False
        else:
            # Insert
            listing = ListingNormalized(
                raw_id=raw_id,
                price=price,
                beds=beds,
                baths=baths,
                property_type=property_type,
                furnished=furnished,
                lease_length_months=lease_length_months,
                lease_length_unknown=lease_length_unknown,
                city_id=city_id,
                county=county,
                area_text=area_text,
                status=status,
            )
            session.add(listing)
            session.flush()
            logger.info("Normalized listing created", normalized_id=listing.id, raw_id=raw_id)
            return listing.id, True


def get_normalized_listing(raw_id: int) -> dict | None:
    """Get normalized listing by raw ID."""
    with get_readonly_session() as session:
        listing = _get_normalized_by_raw_id(session, raw_id)
        if listing:
            return _normalized_to_dict(listing)
        return None


def get_normalized_listing_by_id(normalized_id: int) -> dict | None:
    """Get normalized listing by its own ID."""
    with get_readonly_session() as session:
        listing = session.get(ListingNormalized, normalized_id)
        if listing:
            return _normalized_to_dict(listing)
        return None


def _get_normalized_by_raw_id(session, raw_id: int) -> ListingNormalized | None:
    """Internal: get normalized listing within session."""
    query = select(ListingNormalized).where(ListingNormalized.raw_id == raw_id)
    result = session.execute(query)
    return result.scalar_one_or_none()


def _normalized_to_dict(listing: ListingNormalized) -> dict:
    """Convert ListingNormalized to dictionary."""
    return {
        "id": listing.id,
        "raw_id": listing.raw_id,
        "price": float(listing.price) if listing.price else None,
        "beds": listing.beds,
        "baths": listing.baths,
        "property_type": listing.property_type.value if listing.property_type else None,
        "furnished": listing.furnished,
        "lease_length_months": listing.lease_length_months,
        "lease_length_unknown": listing.lease_length_unknown,
        "city_id": listing.city_id,
        "county": listing.county,
        "area_text": listing.area_text,
        "status": listing.status.value,
        "created_at": listing.created_at,
        "updated_at": listing.updated_at,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# QUERIES
# ═══════════════════════════════════════════════════════════════════════════════


def get_listings_for_city(city_id: int, status: ListingStatus = ListingStatus.ACTIVE) -> list[dict]:
    """Get all listings for a city."""
    with get_readonly_session() as session:
        query = (
            select(ListingNormalized)
            .where(ListingNormalized.city_id == city_id)
            .where(ListingNormalized.status == status)
        )
        result = session.execute(query)
        return [_normalized_to_dict(listing) for listing in result.scalars().all()]


def get_all_normalized_listings() -> list[dict]:
    """
    Get ALL normalized listings for full sync.

    Used by Publisher-ETL on first run when no watermark exists.
    """
    with get_readonly_session() as session:
        query = (
            select(ListingNormalized, ListingRaw)
            .join(ListingRaw, ListingNormalized.raw_id == ListingRaw.id)
            .where(ListingNormalized.status == ListingStatus.ACTIVE)
            .order_by(ListingNormalized.updated_at)
        )
        result = session.execute(query)

        listings = []
        for norm, raw in result.all():
            data = _normalized_to_dict(norm)
            data["source"] = raw.source
            data["url"] = raw.url
            data["first_photo_url"] = raw.first_photo_url
            listings.append(data)

        return listings


def get_listings_updated_since(timestamp: datetime) -> list[dict]:
    """
    Get listings updated since a timestamp.

    Used by Publisher-ETL for incremental sync.
    """
    with get_readonly_session() as session:
        query = (
            select(ListingNormalized, ListingRaw)
            .join(ListingRaw, ListingNormalized.raw_id == ListingRaw.id)
            .where(ListingNormalized.updated_at > timestamp)
            .order_by(ListingNormalized.updated_at)
        )
        result = session.execute(query)

        listings = []
        for norm, raw in result.all():
            data = _normalized_to_dict(norm)
            data["source"] = raw.source
            data["url"] = raw.url
            data["first_photo_url"] = raw.first_photo_url
            listings.append(data)

        return listings


def mark_listing_removed(raw_id: int) -> bool:
    """Mark a listing as removed."""
    with get_session() as session:
        stmt = (
            update(ListingNormalized)
            .where(ListingNormalized.raw_id == raw_id)
            .values(status=ListingStatus.REMOVED, updated_at=datetime.utcnow())
        )
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info("Listing marked as removed", raw_id=raw_id)
            return True
        return False


def get_listing_count(status: ListingStatus | None = None) -> int:
    """Get count of listings."""
    with get_readonly_session() as session:
        query = select(ListingNormalized)
        if status:
            query = query.where(ListingNormalized.status == status)
        result = session.execute(query)
        return len(list(result.scalars().all()))


def get_listings_by_source(source: str, limit: int = 100) -> list[dict]:
    """Get listings by source."""
    with get_readonly_session() as session:
        query = (
            select(ListingNormalized, ListingRaw)
            .join(ListingRaw, ListingNormalized.raw_id == ListingRaw.id)
            .where(ListingRaw.source == source)
            .limit(limit)
        )
        result = session.execute(query)

        listings = []
        for norm, raw in result.all():
            data = _normalized_to_dict(norm)
            data["source"] = raw.source
            data["url"] = raw.url
            listings.append(data)

        return listings
