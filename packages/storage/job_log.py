"""
AGPARS Job Log Storage

CRUD for scraping job logs in ops.job_log.
"""

from typing import Any

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_session
from packages.storage.models import JobStatus

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CREATE
# ═══════════════════════════════════════════════════════════════════════════════


def create_job(
    source: str,
    city_id: int | None = None,
) -> int:
    """
    Create a new job log entry.

    Args:
        source: Source identifier
        city_id: Optional city ID

    Returns:
        New job ID
    """
    with get_session() as session:
        result = session.execute(
            text("""
                INSERT INTO ops.job_log (source, city_id, status, created_at)
                VALUES (:source, :city_id, :status, NOW())
                RETURNING id
            """),
            {
                "source": source,
                "city_id": city_id,
                "status": JobStatus.PENDING.value,
            },
        )
        job_id = result.scalar_one()
        session.commit()
        return job_id


def start_job(job_id: int) -> None:
    """Mark job as running."""
    with get_session() as session:
        session.execute(
            text("""
                UPDATE ops.job_log
                SET status = :status, started_at = NOW()
                WHERE id = :job_id
            """),
            {"job_id": job_id, "status": JobStatus.RUNNING.value},
        )
        session.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# UPDATE
# ═══════════════════════════════════════════════════════════════════════════════


def update_job_status(
    job_id: int,
    status: JobStatus,
    listings_found: int = 0,
    error_message: str | None = None,
) -> None:
    """
    Update job status.

    Args:
        job_id: Job ID
        status: New status
        listings_found: Number of listings found
        error_message: Error message (if failed)
    """
    with get_session() as session:
        session.execute(
            text("""
                UPDATE ops.job_log
                SET
                    status = :status,
                    listings_found = :listings_found,
                    error_message = :error_message,
                    completed_at = CASE WHEN :is_final THEN NOW() ELSE completed_at END
                WHERE id = :job_id
            """),
            {
                "job_id": job_id,
                "status": status.value,
                "listings_found": listings_found,
                "error_message": error_message,
                "is_final": status in (JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.DEAD),
            },
        )
        session.commit()


def complete_job(job_id: int, listings_found: int) -> None:
    """Mark job as successfully completed."""
    update_job_status(job_id, JobStatus.SUCCESS, listings_found=listings_found)


def fail_job(job_id: int, error_message: str) -> None:
    """Mark job as failed."""
    update_job_status(job_id, JobStatus.FAILED, error_message=error_message)


def increment_retry(job_id: int) -> int:
    """Increment retry count and return new count."""
    with get_session() as session:
        result = session.execute(
            text("""
                UPDATE ops.job_log
                SET retry_count = retry_count + 1
                WHERE id = :job_id
                RETURNING retry_count
            """),
            {"job_id": job_id},
        )
        count = result.scalar_one()
        session.commit()
        return count


# ═══════════════════════════════════════════════════════════════════════════════
# READ
# ═══════════════════════════════════════════════════════════════════════════════


def get_job(job_id: int) -> dict | None:
    """Get job by ID."""
    with get_session() as session:
        result = session.execute(
            text("SELECT * FROM ops.job_log WHERE id = :job_id"),
            {"job_id": job_id},
        )
        row = result.fetchone()
        if row:
            return dict(row._mapping)
        return None


def get_recent_jobs(
    source: str | None = None,
    status: JobStatus | None = None,
    limit: int = 20,
) -> list[dict]:
    """Get recent jobs with optional filters."""
    query = "SELECT * FROM ops.job_log WHERE 1=1"
    params: dict[str, Any] = {"limit": limit}

    if source:
        query += " AND source = :source"
        params["source"] = source

    if status:
        query += " AND status = :status"
        params["status"] = status.value

    query += " ORDER BY created_at DESC LIMIT :limit"

    with get_session() as session:
        result = session.execute(text(query), params)
        return [dict(row._mapping) for row in result]


def get_pending_jobs(source: str | None = None) -> list[dict]:
    """Get pending jobs."""
    return get_recent_jobs(source=source, status=JobStatus.PENDING, limit=100)


def get_job_stats(since_hours: int = 24) -> dict:
    """Get job statistics."""
    with get_session() as session:
        result = session.execute(
            text("""
                SELECT
                    source,
                    status,
                    COUNT(*) as count,
                    SUM(listings_found) as total_listings
                FROM ops.job_log
                WHERE created_at > NOW() - INTERVAL '1 hour' * :hours
                GROUP BY source, status
            """),
            {"hours": since_hours},
        )

        stats: dict[str, dict] = {}
        for row in result:
            source = row.source
            if source not in stats:
                stats[source] = {}
            stats[source][row.status] = {
                "count": row.count,
                "listings": row.total_listings or 0,
            }

        return stats


def get_recently_scraped_sources(since_hours: int = 12) -> set[str]:
    """
    Return the set of sources that have at least one successful job
    within the last `since_hours` hours.

    Used by the scheduler to prioritize sources that haven't been
    scraped yet (they get pushed to the front of the queue).
    """
    with get_session() as session:
        result = session.execute(
            text("""
                SELECT DISTINCT source
                FROM ops.job_log
                WHERE status = 'success'
                  AND completed_at > NOW() - INTERVAL '1 hour' * :hours
            """),
            {"hours": since_hours},
        )
        return {row.source for row in result}
