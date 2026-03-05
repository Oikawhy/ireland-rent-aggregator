"""
AGPARS Publisher Metrics

Prometheus metrics for publisher ETL operations.
"""

from prometheus_client import Counter, Gauge, Histogram

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# COUNTERS
# ═══════════════════════════════════════════════════════════════════════════════


# Listings synced to pub schema
LISTINGS_SYNCED = Counter(
    "publisher_listings_synced_total",
    "Total listings synced to pub schema",
    ["source", "status"],  # status: new, updated, removed
)

# Events created in outbox
EVENTS_CREATED = Counter(
    "publisher_events_created_total",
    "Total events created in outbox",
    ["event_type", "workspace_id"],
)

# Sync errors
SYNC_ERRORS = Counter(
    "publisher_sync_errors_total",
    "Total sync errors",
    ["source", "error_type"],
)


# ═══════════════════════════════════════════════════════════════════════════════
# HISTOGRAMS
# ═══════════════════════════════════════════════════════════════════════════════


# Sync duration
SYNC_DURATION = Histogram(
    "publisher_sync_duration_seconds",
    "Time to sync listings to pub schema",
    ["source"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
)

# Batch size
BATCH_SIZE = Histogram(
    "publisher_batch_size",
    "Number of listings per sync batch",
    ["source"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000],
)


# ═══════════════════════════════════════════════════════════════════════════════
# GAUGES
# ═══════════════════════════════════════════════════════════════════════════════


# Last sync timestamp (as Unix epoch)
LAST_SYNC_TIMESTAMP = Gauge(
    "publisher_last_sync_timestamp",
    "Timestamp of last successful sync",
    ["source"],
)

# Pending events in outbox
PENDING_EVENTS = Gauge(
    "publisher_pending_events",
    "Number of pending events in outbox",
    [],
)

# Active listings in pub schema
ACTIVE_LISTINGS = Gauge(
    "publisher_active_listings",
    "Number of active listings in pub schema",
    ["source"],
)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def record_sync_start(source: str) -> None:
    """Record sync start for timing."""
    logger.debug("Sync started", source=source)


def record_sync_complete(
    source: str,
    new_count: int,
    updated_count: int,
    removed_count: int,
    duration_seconds: float,
) -> None:
    """Record sync completion metrics."""
    LISTINGS_SYNCED.labels(source=source, status="new").inc(new_count)
    LISTINGS_SYNCED.labels(source=source, status="updated").inc(updated_count)
    LISTINGS_SYNCED.labels(source=source, status="removed").inc(removed_count)
    SYNC_DURATION.labels(source=source).observe(duration_seconds)
    BATCH_SIZE.labels(source=source).observe(new_count + updated_count)

    import time
    LAST_SYNC_TIMESTAMP.labels(source=source).set(time.time())

    logger.info(
        "Sync completed",
        source=source,
        new=new_count,
        updated=updated_count,
        removed=removed_count,
        duration=duration_seconds,
    )


def record_sync_error(source: str, error_type: str) -> None:
    """Record sync error."""
    SYNC_ERRORS.labels(source=source, error_type=error_type).inc()
    logger.error("Sync error", source=source, error_type=error_type)


def record_event_created(event_type: str, workspace_id: int) -> None:
    """Record event creation in outbox."""
    EVENTS_CREATED.labels(
        event_type=event_type,
        workspace_id=str(workspace_id),
    ).inc()


def update_pending_events(count: int) -> None:
    """Update pending events gauge."""
    PENDING_EVENTS.set(count)


def update_active_listings(source: str, count: int) -> None:
    """Update active listings gauge for source."""
    ACTIVE_LISTINGS.labels(source=source).set(count)


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC STATS
# ═══════════════════════════════════════════════════════════════════════════════


def get_sync_stats() -> dict:
    """
    Get current sync statistics.

    Returns:
        Dict with sync statistics
    """
    return {
        "pending_events": PENDING_EVENTS._value.get(),
        "sources": ["daft", "rent", "myhome", "property", "sherryfitz", "dng"],
    }
