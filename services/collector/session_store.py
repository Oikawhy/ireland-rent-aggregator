"""
AGPARS Session Store Module

Redis-based session storage with TTL for cookie persistence.
"""

import contextlib
import json
from datetime import datetime
from typing import Any

from packages.observability.logger import get_logger
from packages.storage.redis import get_redis_client

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION STORE
# ═══════════════════════════════════════════════════════════════════════════════


class SessionStore:
    """
    Redis-based session storage with TTL.

    Falls back to in-memory storage if Redis is unavailable.
    """

    def __init__(self, redis_client: Any = None):
        self._redis = redis_client
        self._memory_store: dict[str, dict] = {}
        self._key_prefix = "session:"

    def _get_key(self, source: str) -> str:
        """Build Redis key for a source."""
        return f"{self._key_prefix}{source}"

    def save_session(
        self,
        source: str,
        data: dict,
        ttl_hours: int = 72,
    ) -> bool:
        """
        Save session data with TTL.

        Args:
            source: Source identifier
            data: Session data (cookies, timestamps, etc.)
            ttl_hours: Time to live in hours

        Returns:
            True if saved successfully
        """
        key = self._get_key(source)
        ttl_seconds = ttl_hours * 3600

        try:
            # Try Redis first
            redis = self._get_redis()
            if redis:
                serialized = json.dumps(data, default=str)
                redis.setex(key, ttl_seconds, serialized)
                logger.debug("Session saved to Redis", source=source, ttl_hours=ttl_hours)
                return True
        except Exception as e:
            logger.warning("Redis save failed, using memory", error=str(e))

        # Fallback to memory
        self._memory_store[source] = {
            "data": data,
            "expires_at": datetime.utcnow().timestamp() + ttl_seconds,
        }
        logger.debug("Session saved to memory", source=source)
        return True

    def load_session(self, source: str) -> dict | None:
        """
        Load session data.

        Returns:
            Session data dict or None if not found/expired
        """
        key = self._get_key(source)

        try:
            # Try Redis first
            redis = self._get_redis()
            if redis:
                data = redis.get(key)
                if data:
                    return json.loads(data)
        except Exception as e:
            logger.warning("Redis load failed, trying memory", error=str(e))

        # Fallback to memory
        stored = self._memory_store.get(source)
        if stored:
            # Check expiration
            if stored["expires_at"] > datetime.utcnow().timestamp():
                return stored["data"]
            else:
                # Expired
                del self._memory_store[source]

        return None

    def invalidate_session(self, source: str) -> bool:
        """
        Remove session data.

        Returns:
            True if session was removed
        """
        key = self._get_key(source)

        try:
            redis = self._get_redis()
            if redis:
                redis.delete(key)
                logger.info("Session invalidated in Redis", source=source)
        except Exception as e:
            logger.warning("Redis delete failed", error=str(e))

        # Also remove from memory
        if source in self._memory_store:
            del self._memory_store[source]
            logger.info("Session invalidated in memory", source=source)

        return True

    def exists(self, source: str) -> bool:
        """Check if a session exists."""
        return self.load_session(source) is not None

    def get_all_sessions(self) -> list[str]:
        """Get list of all stored session sources."""
        sources = []

        try:
            redis = self._get_redis()
            if redis:
                keys = redis.keys(f"{self._key_prefix}*")
                for k in keys:
                    # Handle both str (decode_responses=True) and bytes
                    key_str = k if isinstance(k, str) else k.decode()
                    sources.append(key_str.replace(self._key_prefix, ""))
        except Exception as e:
            logger.warning("Redis keys failed", error=str(e))

        # Add memory store sources
        sources.extend(self._memory_store.keys())

        return list(set(sources))

    def clear_all(self) -> int:
        """
        Clear all sessions.

        Returns:
            Number of sessions cleared
        """
        count = 0

        try:
            redis = self._get_redis()
            if redis:
                keys = redis.keys(f"{self._key_prefix}*")
                if keys:
                    count = redis.delete(*keys)
        except Exception as e:
            logger.warning("Redis clear failed", error=str(e))

        # Clear memory
        mem_count = len(self._memory_store)
        self._memory_store.clear()

        logger.info("Sessions cleared", redis_count=count, memory_count=mem_count)
        return count + mem_count

    def _get_redis(self) -> Any:
        """Get Redis client (lazy initialization)."""
        if self._redis is None:
            with contextlib.suppress(Exception):
                self._redis = get_redis_client()
        return self._redis


# ═══════════════════════════════════════════════════════════════════════════════
# COOKIE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def serialize_cookies(cookies: list[dict]) -> str:
    """Serialize cookies to JSON string."""
    return json.dumps(cookies, default=str)


def deserialize_cookies(data: str) -> list[dict]:
    """Deserialize cookies from JSON string."""
    return json.loads(data)


def filter_cookies(cookies: list[dict], domain: str) -> list[dict]:
    """Filter cookies by domain."""
    return [c for c in cookies if domain in c.get("domain", "")]
