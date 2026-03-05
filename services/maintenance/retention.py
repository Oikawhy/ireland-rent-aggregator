"""
AGPARS Retention/Cleanup Job

Scheduled cleanup of old data based on retention policies.
"""

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select

from packages.core.config import get_settings
from packages.observability.logger import get_logger
from packages.storage.db import get_session
from packages.storage.models import (
    DeliveryLog,
    EventOutbox,
    EventStatus,
    JobLog,
    ListingNormalized,
    ListingRaw,
    ListingStatus,
)

logger = get_logger(__name__)


def run_retention_cleanup(dry_run: bool = False) -> dict[str, int]:
    """
    Run all retention cleanup tasks.

    Args:
        dry_run: If True, only count records without deleting

    Returns:
        Dict with counts of records cleaned per table
    """
    settings = get_settings()
    results = {}

    # Clean old listings
    results["listings"] = cleanup_old_listings(
        days=settings.retention.listings_days,
        dry_run=dry_run,
    )

    # Clean delivered events
    results["events_delivered"] = cleanup_delivered_events(
        days=settings.retention.events_delivered_days,
        dry_run=dry_run,
    )

    # Clean dead events
    results["events_dead"] = cleanup_dead_events(
        days=settings.retention.events_dead_days,
        dry_run=dry_run,
    )

    # Clean delivery logs
    results["delivery_log"] = cleanup_delivery_logs(
        days=settings.retention.delivery_log_days,
        dry_run=dry_run,
    )

    # Clean job logs
    results["job_log"] = cleanup_job_logs(
        days=settings.retention.job_log_days,
        dry_run=dry_run,
    )

    logger.info(
        "Retention cleanup complete",
        dry_run=dry_run,
        results=results,
    )

    return results


def cleanup_old_listings(days: int, dry_run: bool = False, batch_size: int = 1000) -> int:
    """
    Clean listings not seen for `days` days.

    Args:
        days: Days since last_seen
        dry_run: Count only
        batch_size: Records to delete per batch (default 1000)

    Returns:
        Number of records deleted/counted
    """
    from sqlalchemy import func

    cutoff = datetime.now(UTC) - timedelta(days=days)
    total_deleted = 0

    with get_session() as session:
        # Count total first
        count_query = select(func.count()).select_from(ListingRaw).where(ListingRaw.last_seen < cutoff)
        total_count = session.execute(count_query).scalar()

        if dry_run or total_count == 0:
            logger.info("Would clean old listings", count=total_count, days=days)
            return total_count

        # Delete in batches
        while True:
            # Get batch of IDs
            id_query = select(ListingRaw.id).where(ListingRaw.last_seen < cutoff).limit(batch_size)
            batch_ids = [row[0] for row in session.execute(id_query).fetchall()]

            if not batch_ids:
                break

            # Delete normalized first (FK constraint)
            session.execute(
                delete(ListingNormalized).where(
                    ListingNormalized.raw_id.in_(batch_ids)
                )
            )
            # Then delete raw
            session.execute(
                delete(ListingRaw).where(ListingRaw.id.in_(batch_ids))
            )
            session.commit()

            total_deleted += len(batch_ids)
            logger.debug("Batch cleaned", deleted=total_deleted, total=total_count)

            if len(batch_ids) < batch_size:
                break

        logger.info("Cleaned old listings", count=total_deleted, days=days)
        return total_deleted


def cleanup_delivered_events(days: int, dry_run: bool = False, batch_size: int = 1000) -> int:
    """
    Clean delivered events older than `days` days.

    Args:
        days: Days since processed_at
        dry_run: Count only
        batch_size: Records to delete per batch

    Returns:
        Number of records deleted/counted
    """
    from sqlalchemy import func

    cutoff = datetime.now(UTC) - timedelta(days=days)
    total_deleted = 0

    with get_session() as session:
        # Count total first
        count_query = (
            select(func.count())
            .select_from(EventOutbox)
            .where(EventOutbox.status == EventStatus.DELIVERED)
            .where(EventOutbox.processed_at < cutoff)
        )
        total_count = session.execute(count_query).scalar()

        if dry_run or total_count == 0:
            logger.info("Would clean delivered events", count=total_count, days=days)
            return total_count

        # Delete in batches
        while True:
            id_query = (
                select(EventOutbox.id)
                .where(EventOutbox.status == EventStatus.DELIVERED)
                .where(EventOutbox.processed_at < cutoff)
                .limit(batch_size)
            )
            batch_ids = [row[0] for row in session.execute(id_query).fetchall()]

            if not batch_ids:
                break

            session.execute(delete(EventOutbox).where(EventOutbox.id.in_(batch_ids)))
            session.commit()

            total_deleted += len(batch_ids)
            logger.debug("Batch cleaned (delivered events)", deleted=total_deleted, total=total_count)

            if len(batch_ids) < batch_size:
                break

        logger.info("Cleaned delivered events", count=total_deleted, days=days)
        return total_deleted


def cleanup_dead_events(days: int, dry_run: bool = False, batch_size: int = 1000) -> int:
    """
    Clean dead events older than `days` days.

    Args:
        days: Days since created_at
        dry_run: Count only
        batch_size: Records to delete per batch

    Returns:
        Number of records deleted/counted
    """
    from sqlalchemy import func

    cutoff = datetime.now(UTC) - timedelta(days=days)
    total_deleted = 0

    with get_session() as session:
        count_query = (
            select(func.count())
            .select_from(EventOutbox)
            .where(EventOutbox.status == EventStatus.DEAD)
            .where(EventOutbox.created_at < cutoff)
        )
        total_count = session.execute(count_query).scalar()

        if dry_run or total_count == 0:
            logger.info("Would clean dead events", count=total_count, days=days)
            return total_count

        while True:
            id_query = (
                select(EventOutbox.id)
                .where(EventOutbox.status == EventStatus.DEAD)
                .where(EventOutbox.created_at < cutoff)
                .limit(batch_size)
            )
            batch_ids = [row[0] for row in session.execute(id_query).fetchall()]

            if not batch_ids:
                break

            session.execute(delete(EventOutbox).where(EventOutbox.id.in_(batch_ids)))
            session.commit()

            total_deleted += len(batch_ids)
            logger.debug("Batch cleaned (dead events)", deleted=total_deleted, total=total_count)

            if len(batch_ids) < batch_size:
                break

        logger.info("Cleaned dead events", count=total_deleted, days=days)
        return total_deleted


def cleanup_delivery_logs(days: int, dry_run: bool = False, batch_size: int = 1000) -> int:
    """
    Clean delivery logs older than `days` days.

    Args:
        days: Days since sent_at
        dry_run: Count only
        batch_size: Records to delete per batch

    Returns:
        Number of records deleted/counted
    """
    from sqlalchemy import func

    cutoff = datetime.now(UTC) - timedelta(days=days)
    total_deleted = 0

    with get_session() as session:
        count_query = (
            select(func.count())
            .select_from(DeliveryLog)
            .where(DeliveryLog.sent_at < cutoff)
        )
        total_count = session.execute(count_query).scalar()

        if dry_run or total_count == 0:
            logger.info("Would clean delivery logs", count=total_count, days=days)
            return total_count

        while True:
            id_query = select(DeliveryLog.id).where(DeliveryLog.sent_at < cutoff).limit(batch_size)
            batch_ids = [row[0] for row in session.execute(id_query).fetchall()]

            if not batch_ids:
                break

            session.execute(delete(DeliveryLog).where(DeliveryLog.id.in_(batch_ids)))
            session.commit()

            total_deleted += len(batch_ids)
            logger.debug("Batch cleaned (delivery logs)", deleted=total_deleted, total=total_count)

            if len(batch_ids) < batch_size:
                break

        logger.info("Cleaned delivery logs", count=total_deleted, days=days)
        return total_deleted


def cleanup_job_logs(days: int, dry_run: bool = False, batch_size: int = 1000) -> int:
    """
    Clean job logs older than `days` days.

    Args:
        days: Days since created_at
        dry_run: Count only
        batch_size: Records to delete per batch

    Returns:
        Number of records deleted/counted
    """
    from sqlalchemy import func

    cutoff = datetime.now(UTC) - timedelta(days=days)
    total_deleted = 0

    with get_session() as session:
        count_query = (
            select(func.count())
            .select_from(JobLog)
            .where(JobLog.created_at < cutoff)
        )
        total_count = session.execute(count_query).scalar()

        if dry_run or total_count == 0:
            logger.info("Would clean job logs", count=total_count, days=days)
            return total_count

        while True:
            id_query = select(JobLog.id).where(JobLog.created_at < cutoff).limit(batch_size)
            batch_ids = [row[0] for row in session.execute(id_query).fetchall()]

            if not batch_ids:
                break

            session.execute(delete(JobLog).where(JobLog.id.in_(batch_ids)))
            session.commit()

            total_deleted += len(batch_ids)
            logger.debug("Batch cleaned (job logs)", deleted=total_deleted, total=total_count)

            if len(batch_ids) < batch_size:
                break

        logger.info("Cleaned job logs", count=total_deleted, days=days)
        return total_deleted


def mark_stale_listings(days: int = 7) -> int:
    """
    Mark listings not seen in `days` days as REMOVED.

    Returns:
        Number of listings marked
    """
    cutoff = datetime.now(UTC) - timedelta(days=days)

    with get_session() as session:
        from sqlalchemy import update

        result = session.execute(
            update(ListingNormalized)
            .where(ListingNormalized.status == ListingStatus.ACTIVE)
            .where(
                ListingNormalized.raw_id.in_(
                    select(ListingRaw.id).where(ListingRaw.last_seen < cutoff)
                )
            )
            .values(status=ListingStatus.REMOVED)
        )

        if result.rowcount > 0:
            logger.info("Marked stale listings as removed", count=result.rowcount)

        return result.rowcount

