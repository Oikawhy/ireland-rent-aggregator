"""
AGPARS Watermark Tracker

Tracks sync watermarks for incremental updates.
"""

import contextlib
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_session_context

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# WATERMARK TYPES
# ═══════════════════════════════════════════════════════════════════════════════


WATERMARK_KEYS = {
    "normalizer_sync": "Last normalization sync timestamp",
    "publisher_sync": "Last pub schema sync timestamp",
    "notifier_sync": "Last notification delivery timestamp",
    "collector_daft": "Last Daft.ie scrape timestamp",
    "collector_rent": "Last Rent.ie scrape timestamp",
    "collector_myhome": "Last MyHome.ie scrape timestamp",
    "collector_property": "Last Property.ie scrape timestamp",
    "collector_sherryfitz": "Last SherryFitz scrape timestamp",
    "collector_dng": "Last DNG scrape timestamp",
}


# ═══════════════════════════════════════════════════════════════════════════════
# WATERMARK OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def get_watermark(key: str) -> datetime | None:
    """
    Get a watermark timestamp.

    Args:
        key: Watermark key

    Returns:
        Timestamp or None if not set
    """
    try:
        with get_session_context() as session:
            result = session.execute(
                text("""
                    SELECT value
                    FROM ops.watermarks
                    WHERE key = :key
                """),
                {"key": key},
            )
            row = result.fetchone()

            if row and row.value:
                # Parse ISO format timestamp
                return datetime.fromisoformat(row.value)

            return None

    except Exception as e:
        logger.error("Failed to get watermark", key=key, error=str(e))
        return None


def set_watermark(key: str, timestamp: datetime | None = None, reset: bool = False) -> bool:
    """
    Set a watermark timestamp.

    Args:
        key: Watermark key
        timestamp: Timestamp to set (defaults to now)
        reset: If True, delete the watermark (for force full sync)

    Returns:
        True if set successfully
    """
    # Handle reset (delete watermark to trigger full sync)
    if reset:
        return delete_watermark(key)

    if timestamp is None:
        timestamp = datetime.now(UTC)

    # Ensure UTC
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)

    try:
        with get_session_context() as session:
            session.execute(
                text("""
                    INSERT INTO ops.watermarks (key, value, updated_at)
                    VALUES (:key, :value, NOW())
                    ON CONFLICT (key)
                    DO UPDATE SET
                        value = EXCLUDED.value,
                        updated_at = NOW()
                """),
                {
                    "key": key,
                    "value": timestamp.isoformat(),
                },
            )
            session.commit()

        logger.debug("Watermark set", key=key, timestamp=timestamp.isoformat())
        return True

    except Exception as e:
        logger.error("Failed to set watermark", key=key, error=str(e))
        return False


def get_or_create_watermark(
    key: str,
    default_hours_ago: int = 24,
) -> datetime:
    """
    Get watermark or create with default.

    Args:
        key: Watermark key
        default_hours_ago: Hours ago for default value

    Returns:
        Watermark timestamp
    """
    existing = get_watermark(key)
    if existing:
        return existing

    # Create default
    from datetime import timedelta

    default = datetime.now(UTC) - timedelta(hours=default_hours_ago)
    set_watermark(key, default)
    return default


def delete_watermark(key: str) -> bool:
    """Delete a watermark."""
    try:
        with get_session_context() as session:
            result = session.execute(
                text("DELETE FROM ops.watermarks WHERE key = :key"),
                {"key": key},
            )
            session.commit()
            return result.rowcount > 0

    except Exception as e:
        logger.error("Failed to delete watermark", key=key, error=str(e))
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def get_all_watermarks() -> dict[str, datetime]:
    """Get all watermarks."""
    try:
        with get_session_context() as session:
            result = session.execute(text("SELECT key, value FROM ops.watermarks"))

            watermarks = {}
            for row in result:
                if row.value:
                    with contextlib.suppress(ValueError):
                        watermarks[row.key] = datetime.fromisoformat(row.value)

            return watermarks

    except Exception as e:
        logger.error("Failed to get all watermarks", error=str(e))
        return {}


def get_collector_watermarks() -> dict[str, datetime]:
    """Get all collector-related watermarks."""
    all_marks = get_all_watermarks()
    return {
        k: v for k, v in all_marks.items()
        if k.startswith("collector_")
    }


def reset_all_watermarks() -> int:
    """Reset all watermarks (for testing/debugging)."""
    try:
        with get_session_context() as session:
            result = session.execute(text("DELETE FROM ops.watermarks"))
            session.commit()
            return result.rowcount

    except Exception as e:
        logger.error("Failed to reset watermarks", error=str(e))
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


class WatermarkContext:
    """Context manager for watermark-based sync operations."""

    def __init__(self, key: str, update_on_success: bool = True):
        self.key = key
        self.update_on_success = update_on_success
        self.start_time: datetime | None = None
        self.since: datetime | None = None

    def __enter__(self) -> "WatermarkContext":
        self.start_time = datetime.now(UTC)
        self.since = get_watermark(self.key)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None and self.update_on_success:
            set_watermark(self.key, self.start_time)

    def get_since(self) -> datetime | None:
        return self.since


def with_watermark(key: str) -> WatermarkContext:
    """Create a watermark context for sync operations."""
    return WatermarkContext(key)
