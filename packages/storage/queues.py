"""
AGPARS Queue Module

Job queue primitives for the scraping pipeline.
Implements distributed locks, deduplication, and FIFO job queue.
"""

import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime

from packages.observability.logger import get_logger
from packages.observability.metrics import CRAWL_QUEUE_DEPTH, RETRY_QUEUE_DEPTH
from packages.storage.redis import Keys, get_redis_client

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# JOB DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class Job:
    """Scraping job definition."""

    id: str
    source: str
    city_id: int | None = None
    city_name: str | None = None
    county: str | None = None
    retry_count: int = 0
    created_at: str = ""
    scheduled_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.utcnow().isoformat()
        if not self.scheduled_at:
            self.scheduled_at = self.created_at

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "Job":
        return cls(**json.loads(data))


# ═══════════════════════════════════════════════════════════════════════════════
# DISTRIBUTED LOCK
# ═══════════════════════════════════════════════════════════════════════════════


class DistributedLock:
    """
    Redis-based distributed lock.

    Usage:
        with DistributedLock("job:123", timeout=60):
            # Critical section
    """

    def __init__(self, name: str, timeout: int = 300, blocking: bool = True):
        self.name = name
        self.key = Keys.lock(name)
        self.timeout = timeout
        self.blocking = blocking
        self.client = get_redis_client()
        self._acquired = False

    def acquire(self) -> bool:
        """Attempt to acquire the lock."""
        acquired = self.client.set(
            self.key,
            str(time.time()),
            nx=True,
            ex=self.timeout,
        )
        self._acquired = bool(acquired)
        if self._acquired:
            logger.debug("Lock acquired", lock=self.name)
        return self._acquired

    def release(self) -> None:
        """Release the lock."""
        if self._acquired:
            self.client.delete(self.key)
            logger.debug("Lock released", lock=self.name)
            self._acquired = False

    def __enter__(self) -> "DistributedLock":
        if self.blocking:
            # Simple blocking retry
            max_attempts = 10
            for _ in range(max_attempts):
                if self.acquire():
                    return self
                time.sleep(1)
            raise TimeoutError(f"Could not acquire lock: {self.name}")
        else:
            self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


# ═══════════════════════════════════════════════════════════════════════════════
# DEDUPLICATION
# ═══════════════════════════════════════════════════════════════════════════════


def is_duplicate(key: str, ttl_seconds: int = 3600) -> bool:
    """
    Check if a key has been seen recently (deduplication).

    Args:
        key: Unique identifier for dedup check
        ttl_seconds: How long to remember the key

    Returns:
        True if this is a duplicate (key already exists)
    """
    client = get_redis_client()
    dedup_key = Keys.dedup(key)

    # Returns True if key was NOT set (already exists)
    result = client.set(dedup_key, "1", nx=True, ex=ttl_seconds)
    return result is None


def clear_dedup(key: str) -> None:
    """Clear a deduplication marker."""
    client = get_redis_client()
    client.delete(Keys.dedup(key))


# ═══════════════════════════════════════════════════════════════════════════════
# JOB QUEUE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def enqueue_job(job: Job, priority: bool = False) -> bool:
    """
    Add a job to the queue.

    Args:
        job: Job to enqueue
        priority: If True, push to front of queue (for unscraped sources)

    Returns:
        True if job was added (not duplicate)
    """
    # Check for duplicate
    dedup_key = f"{job.source}:{job.city_id or 'all'}"
    if is_duplicate(dedup_key, ttl_seconds=300):  # 5 min dedup window
        logger.debug("Job deduplicated", job_id=job.id, source=job.source)
        return False

    client = get_redis_client()
    if priority:
        client.lpush(Keys.JOB_QUEUE, job.to_json())
        logger.info(
            "Job enqueued (PRIORITY)",
            job_id=job.id, source=job.source, city=job.city_name,
        )
    else:
        client.rpush(Keys.JOB_QUEUE, job.to_json())
        logger.info(
            "Job enqueued",
            job_id=job.id, source=job.source, city=job.city_name,
        )
    return True


def dequeue_job() -> Job | None:
    """
    Get the next job from the queue.

    Moves job to processing set for reliability.

    Returns:
        Job or None if queue is empty
    """
    client = get_redis_client()

    # Blocking pop with timeout
    result = client.blpop(Keys.JOB_QUEUE, timeout=5)
    if result is None:
        return None

    _, job_data = result
    job = Job.from_json(job_data)

    # Track as processing + record start time for stuck-job recovery
    client.sadd(Keys.JOB_PROCESSING, job.id)
    client.setex(f"{Keys.JOB_STARTED_PREFIX}{job.id}", 3600, str(time.time()))
    logger.debug("Job dequeued", job_id=job.id)

    return job


def complete_job(job: Job) -> None:
    """Mark a job as completed."""
    client = get_redis_client()
    client.srem(Keys.JOB_PROCESSING, job.id)
    client.delete(f"{Keys.JOB_STARTED_PREFIX}{job.id}")
    clear_dedup(f"{job.source}:{job.city_id or 'all'}")
    logger.info("Job completed", job_id=job.id, source=job.source)


def fail_job(job: Job, retry: bool = True) -> None:
    """
    Mark a job as failed.

    Args:
        job: The failed job
        retry: Whether to requeue for retry
    """
    client = get_redis_client()
    client.srem(Keys.JOB_PROCESSING, job.id)

    if retry:
        job.retry_count += 1
        job.scheduled_at = datetime.utcnow().isoformat()
        client.rpush(Keys.JOB_RETRY, job.to_json())
        logger.warning("Job failed, queued for retry", job_id=job.id, retry_count=job.retry_count)
    else:
        logger.error("Job failed permanently", job_id=job.id)


def recover_stuck_jobs(max_age_seconds: int = 1800) -> int:
    """
    Recover jobs stuck in 'processing' for longer than max_age_seconds.

    Jobs get stuck when the collector crashes or loses connectivity
    mid-processing. This function removes them from the processing set
    so they don't block future scheduling.

    Args:
        max_age_seconds: Consider a job stuck after this many seconds (default 30 min)

    Returns:
        Number of recovered (removed) jobs
    """
    client = get_redis_client()
    processing_ids = client.smembers(Keys.JOB_PROCESSING)

    if not processing_ids:
        return 0

    now = time.time()
    recovered = 0

    for job_id in processing_ids:
        started_raw = client.get(f"{Keys.JOB_STARTED_PREFIX}{job_id}")

        if started_raw is None:
            # No timestamp — legacy stuck job, clean it up
            client.srem(Keys.JOB_PROCESSING, job_id)
            recovered += 1
            logger.warning("Recovered stuck job (no timestamp)", job_id=job_id)
            continue

        started_at = float(started_raw)
        age = now - started_at

        if age > max_age_seconds:
            client.srem(Keys.JOB_PROCESSING, job_id)
            client.delete(f"{Keys.JOB_STARTED_PREFIX}{job_id}")
            recovered += 1
            logger.warning(
                "Recovered stuck job",
                job_id=job_id,
                age_minutes=round(age / 60, 1),
            )

    if recovered:
        logger.info("Stuck job recovery complete", recovered=recovered)

    return recovered


def get_queue_stats() -> dict[str, int]:
    """Get queue statistics."""
    client = get_redis_client()
    stats = {
        "pending": client.llen(Keys.JOB_QUEUE),
        "processing": client.scard(Keys.JOB_PROCESSING),
        "retry": client.llen(Keys.JOB_RETRY),
    }

    # Update Prometheus metrics
    CRAWL_QUEUE_DEPTH.set(stats["pending"])
    RETRY_QUEUE_DEPTH.set(stats["retry"])

    return stats
