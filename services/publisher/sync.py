"""
AGPARS Publisher Sync Entry Point

Main entry point for publisher sync operations.

Fixes applied for Phase 3:
1. Full sync on first run (since=None)
2. Correct is_new detection via created_at == updated_at
3. Correct outbox contract: listing_raw_id instead of listing_id
4. Correct subscription→workspace mapping for each event
5. Watermark update only after successful sync
6. Source filter when calling sync_source()
"""

from datetime import UTC, datetime

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.observability.metrics import (
    PUBLISHER_LAG_SECONDS,
    PUBLISHER_LAST_SYNC_TS,
    PUBLISHER_ROWS_UPSERTED,
)
from packages.storage.db import get_readonly_session
from packages.storage.listing_links import create_listing_link
from packages.storage.listings import get_all_normalized_listings, get_listings_updated_since
from services.dedup.linker import compute_similarity, should_link
from services.normalizer.change_detector import ChangeEvent, ChangeType
from services.publisher.change_router import route_batch
from services.publisher.event_outbox import EventType, create_events_batch
from services.publisher.pub_sync import sync_listings_to_pub, sync_removed_listings
from services.publisher.watermark import get_watermark, set_watermark

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════


def run_publisher_sync() -> dict:
    """
    Run the full publisher sync pipeline.

    Steps:
    1. Get listings updated since last watermark (or all if first run)
    2. Sync to pub schema
    3. Detect changes (NEW vs UPDATED)
    4. Route to subscriptions
    5. Create events in outbox
    6. Update watermark (only on success)

    Returns:
        Stats dict
    """
    stats = {
        "listings_processed": 0,
        "new_listings": 0,
        "updated_listings": 0,
        "events_created": 0,
        "subscriptions_matched": 0,
        "skipped_digest_paused": 0,
        "errors": 0,
    }

    sync_start = datetime.now(UTC)
    watermark_key = "publisher_sync"

    try:
        # Get current watermark
        since = get_watermark(watermark_key)

        logger.info("Starting publisher sync", since=since)

        # Step 1: Get listings (full sync if no watermark, incremental otherwise)
        if since is None:
            # First run: full sync
            logger.info("First run detected, performing full sync")
            listings = get_all_normalized_listings()
        else:
            listings = get_listings_updated_since(since)

        stats["listings_processed"] = len(listings)

        if not listings:
            logger.info("No listings to sync")
            # Still update watermark on empty result
            set_watermark(watermark_key, sync_start)
            return stats

        # Step 2: Sync to pub schema
        pub_result = sync_listings_to_pub(since)
        logger.info("Pub sync done", **pub_result)

        # Step 3: Sync removed listings
        removed_result = sync_removed_listings()
        logger.info("Removed sync done", **removed_result)

        # Step 4: Inline cross-source dedup BEFORE creating events
        new_listings = [l for l in listings if _detect_is_new(l)]
        suppressed_raw_ids = _find_cross_source_duplicates(new_listings)
        stats["dedup_suppressed"] = len(suppressed_raw_ids)

        # Step 5: Detect changes and create events
        change_events = []
        for listing in listings:
            is_new = _detect_is_new(listing)

            if is_new:
                event = ChangeEvent(
                    change_type=ChangeType.NEW,
                    field=None,
                    old_value=None,
                    new_value=None,
                )
                stats["new_listings"] += 1
            else:
                event = ChangeEvent(
                    change_type=ChangeType.DETAILS_CHANGED,
                    field="updated",
                    old_value=None,
                    new_value=None,
                )
                stats["updated_listings"] += 1

            change_events.append(event)

        # Step 6: Route to subscriptions
        routing_results = route_batch(listings, change_events)

        # Step 7: Create events in outbox with correct subscription→workspace mapping
        events_to_create = []
        for listing, event, routing in zip(listings, change_events, routing_results, strict=False):
            if not routing.subscription_ids:
                continue

            # Skip listings suppressed by inline dedup
            raw_id = listing.get("raw_id")
            if raw_id in suppressed_raw_ids:
                logger.info(
                    "Notification suppressed (cross-source duplicate)",
                    raw_id=raw_id,
                    source=listing.get("source"),
                )
                continue

            # Use subscription_workspaces mapping for correct workspace per subscription
            for sub_id in routing.subscription_ids:
                # Skip digest/paused subscriptions — DigestScheduler handles those
                delivery_mode = routing.subscription_delivery_modes.get(sub_id, "instant")
                if delivery_mode in ("digest", "paused"):
                    stats["skipped_digest_paused"] += 1
                    continue

                workspace_id = routing.subscription_workspaces.get(sub_id)
                if workspace_id is None:
                    logger.warning("No workspace for subscription", subscription_id=sub_id)
                    continue

                events_to_create.append({
                    "event_type": _map_change_to_event_type(event.change_type),
                    "payload": _build_event_payload(listing, event),
                    "workspace_id": workspace_id,
                    "listing_raw_id": raw_id,
                })

                stats["subscriptions_matched"] += 1

        if events_to_create:
            event_ids = create_events_batch(events_to_create)
            stats["events_created"] = len(event_ids)

        # Step 7: Update watermark ONLY after successful sync
        set_watermark(watermark_key, sync_start)

        # Record metrics - calculate oldest listing timestamp for lag
        oldest_listing_ts = None
        if listings:
            # Find oldest updated_at from processed listings
            timestamps = [lst.get("updated_at") for lst in listings if lst.get("updated_at")]
            if timestamps:
                oldest_listing_ts = min(timestamps)

        _record_metrics(stats, sync_start, oldest_listing_ts)

    except Exception as e:
        logger.error("Publisher sync failed", error=str(e))
        stats["errors"] += 1
        # Do NOT update watermark on failure
        raise

    logger.info("Publisher sync completed", **stats)
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# CHANGE DETECTION
# ═══════════════════════════════════════════════════════════════════════════════


def _detect_is_new(listing: dict) -> bool:
    """
    Detect if listing is NEW or UPDATED.

    Logic: if created_at == updated_at (within 1 second), it's NEW.
    """
    created_at = listing.get("created_at")
    updated_at = listing.get("updated_at")

    if created_at is None or updated_at is None:
        # Fallback: check if we have explicit is_new flag
        return listing.get("is_new", False)

    # Parse if strings
    if isinstance(created_at, str):
        created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    if isinstance(updated_at, str):
        updated_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))

    # Within 1 second = NEW
    diff = abs((updated_at - created_at).total_seconds())
    return diff < 1.0


# ═══════════════════════════════════════════════════════════════════════════════
# INLINE CROSS-SOURCE DEDUP
# ═══════════════════════════════════════════════════════════════════════════════


def _find_cross_source_duplicates(new_listings: list[dict]) -> set[int]:
    """
    Check new listings against existing pub listings for cross-source dupes.

    Runs BEFORE events are created so that duplicate listings from different
    sources do not trigger separate notifications.

    For each new listing:
    1. Query pub.public_listings for active listings from OTHER sources
       with matching county and similar price (±10%)
    2. Verify match using linker.should_link() (address + multi-field similarity)
    3. Create listing_link for confirmed matches
    4. Return raw_ids that should be suppressed (already notified via another source)

    Returns:
        Set of raw_ids whose notification events should be skipped.
    """
    if not new_listings:
        return set()

    suppress: set[int] = set()

    for listing in new_listings:
        raw_id = listing.get("raw_id")
        source = listing.get("source")
        county = listing.get("county")
        price = listing.get("price")

        if not raw_id or not source:
            continue

        # Need at least county or price for a meaningful match
        if not county and not price:
            continue

        try:
            with get_readonly_session() as session:
                # Build targeted query for candidates
                conditions = [
                    "source != :source",
                    "status = 'active'",
                ]
                params: dict = {"source": source}

                if county:
                    conditions.append("LOWER(county) = LOWER(:county)")
                    params["county"] = county

                if price and price > 0:
                    conditions.append("price BETWEEN :price_lo AND :price_hi")
                    params["price_lo"] = price * 0.90
                    params["price_hi"] = price * 1.10

                where = " AND ".join(conditions)
                query = f"""
                    SELECT raw_id, source, price, beds, county, city, area_text
                    FROM pub.public_listings
                    WHERE {where}
                    LIMIT 50
                """
                result = session.execute(text(query), params)
                candidates = [dict(row._mapping) for row in result.fetchall()]

            if not candidates:
                continue

            for candidate in candidates:
                if should_link(listing, candidate):
                    # Confirmed cross-source duplicate
                    suppress.add(raw_id)

                    # Create listing_link for future dedup cycles
                    try:
                        score = compute_similarity(listing, candidate)
                        create_listing_link(
                            raw_id_a=raw_id,
                            raw_id_b=candidate["raw_id"],
                            confidence=score,
                            reason="inline_dedup",
                        )
                        logger.info(
                            "Inline dedup: cross-source match found",
                            new_raw_id=raw_id,
                            new_source=source,
                            match_raw_id=candidate["raw_id"],
                            match_source=candidate["source"],
                            score=score,
                        )
                    except Exception as e:
                        logger.warning(
                            "Inline dedup: link creation failed",
                            error=str(e),
                        )

                    break  # One match is enough to suppress

        except Exception as e:
            logger.warning(
                "Inline dedup check failed for listing",
                raw_id=raw_id,
                error=str(e),
            )

    if suppress:
        logger.info(
            "Inline dedup complete",
            checked=len(new_listings),
            suppressed=len(suppress),
        )

    return suppress


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _map_change_to_event_type(change_type: ChangeType) -> EventType:
    """Map change type to event type.

    Note: EventType only has NEW and UPDATED per migration schema.
    PRICE_CHANGED and REMOVED are mapped to UPDATED since they
    represent updates to existing listings.
    """
    if change_type == ChangeType.NEW:
        return EventType.NEW
    # All other changes (UPDATED, PRICE_CHANGED, REMOVED) map to UPDATED
    return EventType.UPDATED


def _build_event_payload(listing: dict, event: ChangeEvent) -> dict:
    """Build event payload from listing and change."""
    return {
        "listing_id": listing.get("id"),
        "raw_id": listing.get("raw_id"),
        "source": listing.get("source"),
        "url": listing.get("url"),
        "price": str(listing.get("price")) if listing.get("price") else None,
        "beds": listing.get("beds"),
        "baths": listing.get("baths"),
        "property_type": listing.get("property_type"),
        "city": listing.get("city") or listing.get("area_text"),
        "county": listing.get("county"),
        "area_text": listing.get("area_text"),
        "first_photo_url": listing.get("first_photo_url"),
        "furnished": listing.get("furnished"),
        "lease_length_months": listing.get("lease_length_months"),
        "lease_length_unknown": listing.get("lease_length_unknown", False),
        "change_type": event.change_type.value,
        "change_field": event.field,
        "old_value": str(event.old_value) if event.old_value else None,
        "new_value": str(event.new_value) if event.new_value else None,
    }


def _record_metrics(stats: dict, sync_start: datetime, oldest_listing_ts: datetime | None = None) -> None:
    """Record Prometheus metrics.

    Args:
        stats: Sync statistics
        sync_start: When this sync run started
        oldest_listing_ts: Timestamp of oldest processed listing (for lag calculation)
    """
    try:
        now = datetime.now(UTC)

        PUBLISHER_LAST_SYNC_TS.set(now.timestamp())
        PUBLISHER_ROWS_UPSERTED.inc(stats["listings_processed"])

        # Lag = time since oldest unsynced record (per ARCHITECT.md)
        # If we have oldest listing timestamp, use that; otherwise use 0
        if oldest_listing_ts:
            lag = (now - oldest_listing_ts).total_seconds()
            PUBLISHER_LAG_SECONDS.set(lag)
        else:
            PUBLISHER_LAG_SECONDS.set(0)
    except Exception:
        pass  # Metrics are best-effort


# ═══════════════════════════════════════════════════════════════════════════════
# PARTIAL SYNC OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def sync_source(source: str) -> dict:
    """
    Sync a specific source only.

    Args:
        source: Source name (daft, rent, etc.)

    Returns:
        Stats dict
    """
    watermark_key = f"publisher_sync_{source}"
    sync_start = datetime.now(UTC)

    stats = {"source": source, "listings": 0, "events": 0}

    try:
        since = get_watermark(watermark_key)

        # Get listings with source filter
        if since is None:
            # First run for this source
            all_listings = get_all_normalized_listings()
        else:
            all_listings = get_listings_updated_since(since)

        # Filter by source
        source_listings = [lst for lst in all_listings if lst.get("source") == source]

        stats["listings"] = len(source_listings)
        logger.info(f"Syncing source {source}", count=len(source_listings))

        if source_listings:
            # Sync only this source's listings to pub
            sync_listings_to_pub(since, source_filter=source)

        # Update watermark after success
        set_watermark(watermark_key, sync_start)

    except Exception as e:
        logger.error(f"Source sync failed: {source}", error=str(e))
        raise

    return stats


def force_full_sync() -> dict:
    """
    Force a full sync ignoring watermarks.

    Returns:
        Stats dict
    """
    logger.warning("Starting forced full sync")

    # Reset watermark to trigger full sync
    set_watermark("publisher_sync", reset=True)

    return run_publisher_sync()
