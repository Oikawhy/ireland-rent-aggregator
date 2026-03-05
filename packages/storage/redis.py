"""
AGPARS Redis Client Module

Connection management and utilities for Redis.
"""

import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

from packages.core.config import get_settings
from packages.observability.logger import get_logger

logger = get_logger(__name__)

_redis_client: redis.Redis | None = None


def get_redis_client() -> redis.Redis:
    """
    Get or create the Redis client.

    Returns:
        Configured Redis client instance
    """
    global _redis_client
    if _redis_client is None:
        settings = get_settings()
        _redis_client = redis.Redis.from_url(
            settings.redis.url,
            decode_responses=True,
            socket_timeout=30,
            socket_connect_timeout=10,
            retry_on_timeout=True,
            retry=Retry(ExponentialBackoff(), 3),
            health_check_interval=30,
        )
        logger.info(
            "Redis client created",
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
        )
    return _redis_client


def verify_redis_connection() -> bool:
    """
    Verify Redis connection is working.

    Returns:
        True if connection is successful
    """
    try:
        client = get_redis_client()
        client.ping()
        logger.info("Redis connection verified")
        return True
    except Exception as e:
        logger.error("Redis connection failed", error=str(e))
        return False


def close_redis_client() -> None:
    """Close the Redis client connection."""
    global _redis_client
    if _redis_client is not None:
        _redis_client.close()
        _redis_client = None
        logger.info("Redis client closed")


def reset_redis_client() -> None:
    """Force-reset the Redis client to trigger a fresh reconnection."""
    global _redis_client
    if _redis_client is not None:
        try:
            _redis_client.close()
        except Exception:
            pass
        _redis_client = None
        logger.warning("Redis client reset — next call will reconnect")


# ═══════════════════════════════════════════════════════════════════════════════
# KEY PATTERNS
# ═══════════════════════════════════════════════════════════════════════════════

class Keys:
    """Redis key patterns for the rental pipeline."""

    # Job queues
    JOB_QUEUE = "agpars:jobs:queue"
    JOB_PROCESSING = "agpars:jobs:processing"
    JOB_RETRY = "agpars:jobs:retry"
    JOB_STARTED_PREFIX = "agpars:jobs:started:"

    # Locks
    LOCK_PREFIX = "agpars:lock:"

    # Deduplication
    DEDUP_PREFIX = "agpars:dedup:"

    # Sessions
    SESSION_PREFIX = "agpars:session:"

    # Metrics
    METRICS_PREFIX = "agpars:metrics:"

    @staticmethod
    def lock(name: str) -> str:
        return f"{Keys.LOCK_PREFIX}{name}"

    @staticmethod
    def dedup(key: str) -> str:
        return f"{Keys.DEDUP_PREFIX}{key}"

    @staticmethod
    def session(source: str) -> str:
        return f"{Keys.SESSION_PREFIX}{source}"
