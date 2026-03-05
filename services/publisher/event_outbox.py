"""
AGPARS Event Outbox (Transactional Outbox Pattern)

Atomically write events with listing data for reliable delivery.

Schema alignment with ops.event_outbox migration:
- id: INT SERIAL (auto-generated)
- workspace_id: INT NOT NULL
- event_type: ENUM('new', 'updated')
- listing_raw_id: INT NOT NULL
- payload: JSONB
- status: ENUM('pending', 'delivering', 'delivered', 'failed', 'dead')
- retry_count: INT
- created_at, processed_at: TIMESTAMP
"""

import json
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.observability.metrics import record_event
from packages.storage.db import get_session_context

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT TYPES & STATUSES (matching migration ENUM)
# ═══════════════════════════════════════════════════════════════════════════════


class EventType(Enum):
    """Types of events in the outbox (matches ops.eventtype ENUM)."""

    NEW = "new"
    UPDATED = "updated"


class EventStatus(Enum):
    """Status of outbox events (matches ops.eventstatus ENUM)."""

    PENDING = "pending"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    FAILED = "failed"
    DEAD = "dead"


# ═══════════════════════════════════════════════════════════════════════════════
# OUTBOX OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def create_event(
    workspace_id: int,
    event_type: EventType | str,
    listing_raw_id: int,
    payload: dict | None = None,
) -> int:
    """
    Create a new event in the outbox.

    Args:
        workspace_id: Target workspace ID (required)
        event_type: Type of event ('new' or 'updated')
        listing_raw_id: Raw listing ID (required)
        payload: Optional event payload data

    Returns:
        Event ID (integer)
    """
    event_type_str = event_type.value if isinstance(event_type, EventType) else event_type

    # Validate event_type
    if event_type_str not in ("new", "updated"):
        raise ValueError(f"Invalid event_type: {event_type_str}. Must be 'new' or 'updated'")

    payload_json = json.dumps(payload or {})

    try:
        with get_session_context() as session:
            result = session.execute(
                text("""
                    INSERT INTO ops.event_outbox (
                        workspace_id,
                        event_type,
                        listing_raw_id,
                        payload,
                        status,
                        retry_count,
                        created_at
                    ) VALUES (
                        :workspace_id,
                        :event_type ::ops.eventtype,
                        :listing_raw_id,
                        :payload ::jsonb,
                        'pending' ::ops.eventstatus,
                        0,
                        :created_at
                    )
                    RETURNING id
                """),
                {
                    "workspace_id": workspace_id,
                    "event_type": event_type_str,
                    "listing_raw_id": listing_raw_id,
                    "payload": payload_json,
                    "created_at": datetime.now(UTC),
                },
            )
            event_id = result.scalar_one()
            session.commit()

        logger.debug("Event created", event_id=event_id, event_type=event_type_str)
        record_event(event_type_str)
        return event_id

    except Exception as e:
        logger.error("Failed to create event", error=str(e))
        raise


def get_pending_events(
    limit: int = 100,
    workspace_id: int | None = None,
) -> list[dict]:
    """
    Get pending events for processing.

    Args:
        limit: Maximum events to return
        workspace_id: Optional filter by workspace

    Returns:
        List of event dicts
    """
    try:
        with get_session_context() as session:
            query = """
                SELECT
                    id,
                    workspace_id,
                    event_type,
                    listing_raw_id,
                    payload,
                    status,
                    retry_count,
                    created_at,
                    processed_at
                FROM ops.event_outbox
                WHERE status = 'pending'::ops.eventstatus
            """

            params: dict[str, Any] = {"limit": limit}

            if workspace_id:
                query += " AND workspace_id = :workspace_id"
                params["workspace_id"] = workspace_id

            query += " ORDER BY created_at ASC LIMIT :limit"

            result = session.execute(text(query), params)

            events = []
            for row in result:
                events.append({
                    "id": row.id,
                    "workspace_id": row.workspace_id,
                    "event_type": row.event_type,
                    "listing_raw_id": row.listing_raw_id,
                    "payload": row.payload,
                    "status": row.status,
                    "retry_count": row.retry_count,
                    "created_at": row.created_at,
                    "processed_at": row.processed_at,
                })

            return events

    except Exception as e:
        logger.error("Failed to get pending events", error=str(e))
        return []


def mark_event_delivering(event_id: int) -> bool:
    """
    Mark event as delivering (claim for delivery).

    State machine: PENDING → DELIVERING (only)

    Returns:
        True if transition successful, False if invalid state or error
    """
    try:
        with get_session_context() as session:
            result = session.execute(
                text("""
                    UPDATE ops.event_outbox
                    SET
                        status = 'delivering'::ops.eventstatus,
                        processed_at = :processed_at
                    WHERE id = :event_id
                    AND status = 'pending'::ops.eventstatus
                """),
                {
                    "event_id": event_id,
                    "processed_at": datetime.now(UTC),
                },
            )
            session.commit()

            if result.rowcount == 0:
                logger.warning(
                    "Invalid state transition to DELIVERING",
                    event_id=event_id,
                    reason="Event not in PENDING status",
                )
                return False

            return True

    except Exception as e:
        logger.error("Failed to mark event delivering", error=str(e))
        return False


def mark_event_delivered(event_id: int) -> bool:
    """
    Mark event as delivered (success).

    State machine: DELIVERING → DELIVERED

    Returns:
        True if transition successful, False if invalid state or error
    """
    try:
        with get_session_context() as session:
            result = session.execute(
                text("""
                    UPDATE ops.event_outbox
                    SET
                        status = 'delivered'::ops.eventstatus,
                        processed_at = :processed_at
                    WHERE id = :event_id
                    AND status = 'delivering'::ops.eventstatus
                """),
                {
                    "event_id": event_id,
                    "processed_at": datetime.now(UTC),
                },
            )
            session.commit()

            if result.rowcount == 0:
                logger.warning(
                    "Invalid state transition to DELIVERED",
                    event_id=event_id,
                    reason="Event not in DELIVERING status",
                )
                return False

            return True

    except Exception as e:
        logger.error("Failed to mark event delivered", error=str(e))
        return False


def mark_event_failed(
    event_id: int,
    max_retries: int = 5,
) -> bool:
    """
    Mark event as failed, increment retry count.
    If retry_count >= max_retries, mark as DEAD.

    Args:
        event_id: Event ID
        max_retries: Maximum retry attempts before DEAD

    Returns:
        True if updated
    """
    try:
        with get_session_context() as session:
            # Get current retry count (only if in DELIVERING status)
            result = session.execute(
                text("""
                    SELECT retry_count FROM ops.event_outbox
                    WHERE id = :id AND status = 'delivering'::ops.eventstatus
                """),
                {"id": event_id},
            )
            row = result.fetchone()

            if not row:
                logger.warning(
                    "Invalid state transition to FAILED",
                    event_id=event_id,
                    reason="Event not in DELIVERING status",
                )
                return False

            retry_count = (row.retry_count or 0) + 1

            # Determine final status
            new_status = "dead" if retry_count >= max_retries else "failed"

            session.execute(
                text("""
                    UPDATE ops.event_outbox
                    SET
                        status = :status::ops.eventstatus,
                        retry_count = :retry_count,
                        processed_at = :processed_at
                    WHERE id = :event_id
                    AND status = 'delivering'::ops.eventstatus
                """),
                {
                    "event_id": event_id,
                    "status": new_status,
                    "retry_count": retry_count,
                    "processed_at": datetime.now(UTC),
                },
            )
            session.commit()
            return True

    except Exception as e:
        logger.error("Failed to mark event failed", error=str(e))
        return False


def retry_failed_events(max_retries: int = 5) -> int:
    """
    Move FAILED events back to PENDING for retry (if retry_count < max_retries).

    State machine: FAILED → PENDING (only)

    This function only transitions events that are:
    - Currently in FAILED status
    - Have retry_count < max_retries

    Args:
        max_retries: Only retry events under this retry count

    Returns:
        Number of events queued for retry
    """
    try:
        with get_session_context() as session:
            result = session.execute(
                text("""
                    UPDATE ops.event_outbox
                    SET status = 'pending'::ops.eventstatus
                    WHERE status = 'failed'::ops.eventstatus
                    AND retry_count < :max_retries
                """),
                {"max_retries": max_retries},
            )
            session.commit()
            count = result.rowcount

            if count > 0:
                logger.info("Events queued for retry", count=count)

            return count

    except Exception as e:
        logger.error("Failed to retry events", error=str(e))
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def create_events_batch(events: list[dict]) -> list[int]:
    """
    Create multiple events in one transaction.

    Args:
        events: List of event dicts with workspace_id, event_type, listing_raw_id, payload

    Returns:
        List of created event IDs
    """
    event_ids = []

    try:
        with get_session_context() as session:
            for event in events:
                event_type = event.get("event_type", EventType.NEW.value)

                if isinstance(event_type, EventType):
                    event_type = event_type.value

                if event_type not in ("new", "updated"):
                    event_type = "new"

                payload_json = json.dumps(event.get("payload", {}))

                result = session.execute(
                    text("""
                        INSERT INTO ops.event_outbox (
                            workspace_id, event_type, listing_raw_id, payload,
                            status, retry_count, created_at
                        ) VALUES (
                            :workspace_id,
                            :event_type ::ops.eventtype,
                            :listing_raw_id,
                            :payload ::jsonb,
                            'pending' ::ops.eventstatus,
                            0,
                            :created_at
                        )
                        RETURNING id
                    """),
                    {
                        "workspace_id": event["workspace_id"],
                        "event_type": event_type,
                        "listing_raw_id": event["listing_raw_id"],
                        "payload": payload_json,
                        "created_at": datetime.now(UTC),
                    },
                )
                event_ids.append(result.scalar_one())

            session.commit()

        logger.info("Batch events created", count=len(event_ids))
        # Record stats for batch (approximate as we don't track type counts in batch explicitly in this function version, 
        # but we can assume mostly new or track individually if needed. For now, simplistic loop to record.)
        for event in events:
            evt_type = event.get("event_type", "new")
            record_event(evt_type.value if isinstance(evt_type, EventType) else evt_type)
            
        return event_ids

    except Exception as e:
        logger.error("Failed to create batch events", error=str(e))
        raise


def cleanup_old_events(days_old: int = 7) -> int:
    """
    Clean up old delivered/dead events.

    Args:
        days_old: Delete events older than this many days

    Returns:
        Number of deleted events
    """
    try:
        with get_session_context() as session:
            result = session.execute(
                text("""
                    DELETE FROM ops.event_outbox
                    WHERE status IN ('delivered', 'dead')
                    AND created_at < NOW() - INTERVAL '1 day' * :days
                """),
                {"days": days_old},
            )
            session.commit()
            return result.rowcount

    except Exception as e:
        logger.error("Failed to cleanup events", error=str(e))
        return 0


def get_outbox_stats() -> dict:
    """Get outbox statistics."""
    try:
        with get_session_context() as session:
            result = session.execute(text("""
                SELECT
                    status::text,
                    COUNT(*) as count
                FROM ops.event_outbox
                GROUP BY status
            """))

            status_counts = {row.status: row.count for row in result}

            result = session.execute(text("""
                SELECT
                    event_type::text,
                    COUNT(*) as count
                FROM ops.event_outbox
                WHERE status = 'pending'
                GROUP BY event_type
            """))

            pending_by_type = {row.event_type: row.count for row in result}

            return {
                "by_status": status_counts,
                "pending_by_type": pending_by_type,
            }

    except Exception as e:
        logger.error("Failed to get outbox stats", error=str(e))
        return {}
