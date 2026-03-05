"""
AGPARS Telegram Rate Limiter

Redis-based sliding window rate limiter for Telegram API.
Enforces per-chat and global broadcast limits.

Covers T064.
"""

import time

from packages.observability.logger import get_logger
from packages.storage.redis import Keys, get_redis_client

logger = get_logger(__name__)


# Telegram API limits
GLOBAL_RATE_LIMIT = 30       # messages per second (broadcast)
PER_CHAT_RATE_LIMIT = 1      # message per second per chat
RATE_WINDOW_SECONDS = 1      # sliding window size


class RateLimiter:
    """
    Redis sliding-window rate limiter for Telegram delivery.

    Uses sorted sets with timestamps for precise windowing.
    """

    GLOBAL_KEY = "agpars:ratelimit:global"
    CHAT_KEY_PREFIX = "agpars:ratelimit:chat:"

    def __init__(
        self,
        global_limit: int = GLOBAL_RATE_LIMIT,
        per_chat_limit: int = PER_CHAT_RATE_LIMIT,
        window_seconds: float = RATE_WINDOW_SECONDS,
    ):
        self.global_limit = global_limit
        self.per_chat_limit = per_chat_limit
        self.window_seconds = window_seconds

    def can_send(self, chat_id: int) -> bool:
        """
        Check if a message can be sent to the given chat.

        Returns:
            True if both global and per-chat limits allow sending
        """
        now = time.time()
        client = get_redis_client()

        # Check global limit
        if not self._check_window(client, self.GLOBAL_KEY, self.global_limit, now):
            return False

        # Check per-chat limit
        chat_key = f"{self.CHAT_KEY_PREFIX}{chat_id}"
        return self._check_window(client, chat_key, self.per_chat_limit, now)

    def record_send(self, chat_id: int) -> None:
        """Record a sent message for rate tracking."""
        now = time.time()
        client = get_redis_client()
        member = f"{now}:{chat_id}"

        pipe = client.pipeline()

        # Add to global window
        pipe.zadd(self.GLOBAL_KEY, {member: now})
        pipe.expire(self.GLOBAL_KEY, int(self.window_seconds) + 5)

        # Add to per-chat window
        chat_key = f"{self.CHAT_KEY_PREFIX}{chat_id}"
        pipe.zadd(chat_key, {member: now})
        pipe.expire(chat_key, int(self.window_seconds) + 5)

        pipe.execute()

    def wait_for_slot(self, chat_id: int, max_wait: float = 5.0) -> bool:
        """
        Wait until a slot is available.

        Args:
            chat_id: Target chat ID
            max_wait: Maximum seconds to wait

        Returns:
            True if slot became available, False if timed out
        """
        start = time.time()
        while time.time() - start < max_wait:
            if self.can_send(chat_id):
                return True
            time.sleep(0.05)  # 50ms poll
        logger.warning("Rate limit wait timed out", chat_id=chat_id)
        return False

    def _check_window(
        self,
        client,
        key: str,
        limit: int,
        now: float,
    ) -> bool:
        """Check if count within window is under limit."""
        window_start = now - self.window_seconds

        # Remove expired entries and count remaining
        pipe = client.pipeline()
        pipe.zremrangebyscore(key, "-inf", window_start)
        pipe.zcard(key)
        results = pipe.execute()

        current_count = results[1]
        return current_count < limit

    def get_stats(self) -> dict:
        """Get current rate limiter stats."""
        now = time.time()
        client = get_redis_client()
        window_start = now - self.window_seconds

        client.zremrangebyscore(self.GLOBAL_KEY, "-inf", window_start)
        global_count = client.zcard(self.GLOBAL_KEY)

        return {
            "global_count": global_count,
            "global_limit": self.global_limit,
            "window_seconds": self.window_seconds,
            "available": max(0, self.global_limit - global_count),
        }
