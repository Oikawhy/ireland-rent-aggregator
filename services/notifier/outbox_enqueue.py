"""
AGPARS Outbox Enqueue

Creates events in ops.event_outbox for listings that match subscriptions.
Called after normalization detects NEW or UPDATED listings.

Covers T059.
"""

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_session
from packages.storage.models import EventStatus, EventType
from packages.storage.subscriptions import get_active_subscriptions
from services.rules.subscription_filters import matches_subscription

logger = get_logger(__name__)


def enqueue_listing_event(
    raw_id: int,
    event_type: str,
    listing_data: dict,
) -> list[int]:
    """
    Enqueue outbox events for all subscriptions matching this listing.

    Args:
        raw_id: listings_raw.id
        event_type: "new" or "updated"
        listing_data: Normalized listing dict (price, beds, city_id, etc.)

    Returns:
        List of created event_outbox IDs
    """
    subscriptions = get_active_subscriptions()
    if not subscriptions:
        return []

    # Validate event type
    try:
        ev_type = EventType(event_type)
    except ValueError:
        logger.error("Invalid event type", event_type=event_type)
        return []

    created_ids: list[int] = []

    for sub in subscriptions:
        if not matches_subscription(listing_data, sub):
            continue

        # Skip paused and digest subscriptions
        # Digest events are handled by DigestScheduler, not instant delivery
        if sub.get("delivery_mode") in ("paused", "digest"):
            continue

        event_id = _insert_event(
            workspace_id=sub["workspace_id"],
            event_type=ev_type,
            listing_raw_id=raw_id,
            payload=_build_payload(listing_data, ev_type),
        )

        if event_id:
            created_ids.append(event_id)

    if created_ids:
        logger.info(
            "Events enqueued",
            raw_id=raw_id,
            event_type=event_type,
            count=len(created_ids),
        )

    return created_ids


def _insert_event(
    workspace_id: int,
    event_type: EventType,
    listing_raw_id: int,
    payload: dict,
) -> int | None:
    """Insert a single event into ops.event_outbox."""
    try:
        with get_session() as session:
            result = session.execute(
                text("""
                    INSERT INTO ops.event_outbox
                        (workspace_id, event_type, listing_raw_id, payload, status, retry_count, created_at)
                    VALUES
                        (:workspace_id, :event_type, :listing_raw_id, :payload::jsonb,
                         :status, 0, NOW())
                    RETURNING id
                """),
                {
                    "workspace_id": workspace_id,
                    "event_type": event_type.value,
                    "listing_raw_id": listing_raw_id,
                    "payload": __import__("json").dumps(payload),
                    "status": EventStatus.PENDING.value,
                },
            )
            event_id = result.scalar_one()
            session.commit()
            return event_id
    except Exception as e:
        logger.error(
            "Failed to insert outbox event",
            workspace_id=workspace_id,
            raw_id=listing_raw_id,
            error=str(e),
        )
        return None


def _build_payload(listing_data: dict, event_type: EventType) -> dict:
    """Build event payload from listing data."""
    payload = {
        "source": listing_data.get("source"),
        "url": listing_data.get("url"),
        "price": float(listing_data["price"]) if listing_data.get("price") else None,
        "beds": listing_data.get("beds"),
        "baths": listing_data.get("baths"),
        "property_type": listing_data.get("property_type"),
        "city": listing_data.get("city"),
        "county": listing_data.get("county"),
        "area_text": listing_data.get("area_text"),
        "first_photo_url": listing_data.get("first_photo_url"),
        "furnished": listing_data.get("furnished"),
        "lease_length_months": listing_data.get("lease_length_months"),
        "lease_length_unknown": listing_data.get("lease_length_unknown", False),
    }

    if event_type == EventType.UPDATED:
        payload["changes"] = listing_data.get("changes", {})

    return payload
