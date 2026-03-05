"""
AGPARS Job Scheduler

Creates scrape jobs per source × city, coordinates with circuit breaker,
dedup, distributed locks, and job_log persistence.

Covers T043 (job creation) and T044 (job log updates).
"""

import uuid
import socket
from datetime import datetime

from packages.core.config import get_settings
from packages.observability.logger import get_logger
from packages.storage.job_log import (
    create_job, start_job, complete_job, fail_job,
    get_recently_scraped_sources,
)
from packages.storage.queues import Job, DistributedLock, enqueue_job, recover_stuck_jobs
from packages.storage.subscriptions import ensure_default_subscription, get_active_subscriptions
from services.scheduler.retries import is_source_paused

logger = get_logger(__name__)

# All known sources
ALL_SOURCES = ["dng", "sherryfitz", "daft", "rent", "myhome", "property"]


# ═══════════════════════════════════════════════════════════════════════════════
# NETWORK HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════════


def check_network_available(host: str = "1.1.1.1", port: int = 53, timeout: int = 5) -> bool:
    """
    Quick TCP connect to verify external network is reachable.

    Uses Cloudflare DNS (1.1.1.1:53) as a lightweight connectivity probe.
    Returns True if network is available, False otherwise.
    """
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True
    except (OSError, TimeoutError):
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# JOB CREATION  (T043)
# ═══════════════════════════════════════════════════════════════════════════════


def get_source_city_pairs(subscriptions: list[dict]) -> list[tuple[str, int | None, str | None]]:
    """
    Extract unique (source, city_id, city_name) triples from subscriptions.

    If a subscription has no city_ids filter, all sources run with city=None
    (full national scrape).

    Returns pairs ordered by ALL_SOURCES (daft first for priority processing).
    """
    seen: dict[tuple[str, int | None, str | None], None] = {}

    for sub in subscriptions:
        filters = sub.get("filters", {})
        city_ids = filters.get("city_ids", [])
        city_names = filters.get("city_names", [])
        sources = filters.get("sources", ALL_SOURCES)

        for source in sources:
            if source not in ALL_SOURCES:
                continue
            if city_ids:
                for i, cid in enumerate(city_ids):
                    name = city_names[i] if i < len(city_names) else None
                    seen.setdefault((source, cid, name), None)
            else:
                # No city filter → full national scrape
                seen.setdefault((source, None, None), None)

    # Sort by ALL_SOURCES order so daft is always first
    result = list(seen.keys())
    source_order = {s: i for i, s in enumerate(ALL_SOURCES)}
    result.sort(key=lambda t: source_order.get(t[0], 999))
    return result


def create_scrape_jobs() -> list[dict]:
    """
    Create scrape jobs for all active subscriptions.

    Process:
      1. Gather active subscriptions
      2. Extract unique (source, city) pairs
      3. Check which sources were recently scraped
      4. For each: check circuit breaker, acquire lock, enqueue
         (unscraped sources get priority = front of queue)

    Returns:
        List of created job dicts with {job_id, source, city_id, status}
    """
    # ── Recover stuck jobs from previous cycles ────────────────────────────
    try:
        recovered = recover_stuck_jobs(max_age_seconds=1800)  # 30 min
        if recovered:
            logger.warning("Recovered stuck jobs", count=recovered)
    except Exception as e:
        logger.warning("Stuck job recovery failed", error=str(e))

    # ── Network health gate ───────────────────────────────────────────────
    if not check_network_available():
        logger.warning("Network unavailable, skipping job creation")
        return []

    subscriptions = get_active_subscriptions()
    if not subscriptions:
        # Auto-seed a default subscription so the pipeline can bootstrap
        if ensure_default_subscription():
            logger.warning("Auto-seeded default subscription, re-querying...")
            subscriptions = get_active_subscriptions()
        if not subscriptions:
            logger.info("No active subscriptions, skipping job creation")
            return []

    pairs = get_source_city_pairs(subscriptions)
    logger.info("Scheduling jobs", pairs_count=len(pairs))

    # Determine which sources were recently scraped (last 12 hours)
    try:
        recently_scraped = get_recently_scraped_sources(since_hours=12)
        logger.info(
            "Recently scraped sources",
            scraped=sorted(recently_scraped),
            total=len(recently_scraped),
        )
    except Exception as e:
        logger.warning("Could not check recent scrapes, no prioritization", error=str(e))
        recently_scraped = set()

    created: list[dict] = []

    for source, city_id, city_name in pairs:
        # Sources NOT recently scraped get pushed to front of queue
        is_priority = source not in recently_scraped
        result = _schedule_single_job(source, city_id, city_name, priority=is_priority)
        if result:
            created.append(result)

    logger.info("Job creation complete", created=len(created), total_pairs=len(pairs))
    return created


def _schedule_single_job(
    source: str,
    city_id: int | None,
    city_name: str | None,
    priority: bool = False,
) -> dict | None:
    """Schedule a single scrape job with guards."""

    # 1. Circuit breaker check
    if is_source_paused(source):
        logger.debug("Source paused (circuit open)", source=source)
        return None

    # 2. Distributed lock (prevent duplicate scheduling)
    lock_name = f"schedule:{source}:{city_id or 'all'}"
    lock = DistributedLock(lock_name, timeout=120, blocking=False)
    if not lock.acquire():
        logger.debug("Lock held, skipping", source=source, city_id=city_id)
        return None

    try:
        # 3. Create DB job record
        db_job_id = create_job(source=source, city_id=city_id)

        # 4. Create Redis queue job
        queue_job = Job(
            id=f"{source}-{city_id or 'all'}-{uuid.uuid4().hex[:8]}",
            source=source,
            city_id=city_id,
            city_name=city_name,
        )

        enqueued = enqueue_job(queue_job, priority=priority)
        if not enqueued:
            logger.debug("Job deduplicated by queue", source=source, city_id=city_id)
            return None

        logger.info(
            "Job scheduled",
            db_job_id=db_job_id,
            queue_job_id=queue_job.id,
            source=source,
            city=city_name,
            priority=priority,
        )

        return {
            "job_id": db_job_id,
            "queue_job_id": queue_job.id,
            "source": source,
            "city_id": city_id,
            "city_name": city_name,
            "status": "scheduled",
            "priority": priority,
        }

    finally:
        lock.release()


# ═══════════════════════════════════════════════════════════════════════════════
# JOB LOG UPDATES  (T044)
# ═══════════════════════════════════════════════════════════════════════════════


async def run_job_with_logging(runner, job) -> "ScrapeResult":  # noqa: F821
    """
    Wrap CollectorRunner.run_job with job_log lifecycle updates.

    Args:
        runner: CollectorRunner instance
        job: ScrapeJob instance (must have .job_id set)

    Returns:
        ScrapeResult from the runner
    """
    from services.collector.runner import ScrapeResult

    job_id = job.job_id

    if job_id:
        start_job(job_id)
        logger.info("Job started", job_id=job_id, source=job.source)

    try:
        result: ScrapeResult = await runner.run_job(job)

        if job_id:
            if result.success:
                complete_job(job_id, listings_found=len(result.listings))
                logger.info(
                    "Job completed successfully",
                    job_id=job_id,
                    listings=len(result.listings),
                )
            else:
                error_msg = "; ".join(result.errors) if result.errors else "Unknown error"
                fail_job(job_id, error_message=error_msg)
                logger.warning("Job failed", job_id=job_id, errors=result.errors)

        return result

    except Exception as e:
        if job_id:
            fail_job(job_id, error_message=str(e))
            logger.error("Job failed with exception", job_id=job_id, error=str(e))
        raise
