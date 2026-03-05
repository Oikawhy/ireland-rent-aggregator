"""
AGPARS Request Throttling Module

Rate limiting with jitter for source-specific request throttling.
"""

import asyncio
import random
from datetime import datetime, timedelta
from typing import Any

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# RATE LIMITS (per source)
# ═══════════════════════════════════════════════════════════════════════════════


# Default rate limits per source (requests per minute)
SOURCE_RATE_LIMITS = {
    "daft": {"rpm": 30, "min_delay_ms": 2000, "base_delay": 2.0},
    "rent": {"rpm": 20, "min_delay_ms": 3000, "base_delay": 3.0},
    "myhome": {"rpm": 20, "min_delay_ms": 3000, "base_delay": 3.0},
    "property": {"rpm": 20, "min_delay_ms": 3000, "base_delay": 3.0},
    "sherryfitz": {"rpm": 15, "min_delay_ms": 4000, "base_delay": 4.0},
    "dng": {"rpm": 15, "min_delay_ms": 4000, "base_delay": 4.0},
}

# Alias for test compatibility
SOURCE_CONFIGS = SOURCE_RATE_LIMITS

DEFAULT_RATE_LIMIT = {"rpm": 15, "min_delay_ms": 4000, "base_delay": 4.0}


# Standalone helper functions for test compatibility
def random_delay(min_seconds: float, max_seconds: float) -> float:
    """Generate a random delay in seconds (sync version for simple use cases)."""
    return random.uniform(min_seconds, max_seconds)


def add_jitter(base: float, jitter_percent: float = 0.2) -> float:
    """
    Add random jitter to a base delay.

    Args:
        base: Base delay value
        jitter_percent: Percentage of jitter (0.2 = ±20%)

    Returns:
        Base value with jitter applied
    """
    jitter = base * jitter_percent
    return base + random.uniform(-jitter, jitter)


# ═══════════════════════════════════════════════════════════════════════════════
# THROTTLER
# ═══════════════════════════════════════════════════════════════════════════════


class Throttler:
    """
    Request throttler with per-source rate limiting and jitter.

    Uses Redis for distributed rate limiting in multi-worker scenarios.
    Falls back to in-memory tracking for local development.
    """

    def __init__(self):
        self._last_request: dict[str, datetime] = {}
        self._redis = None

    def _get_rate_limit(self, source: str) -> dict:
        """Get rate limit config for a source."""
        return SOURCE_RATE_LIMITS.get(source, DEFAULT_RATE_LIMIT)

    def _apply_jitter(self, delay_ms: int) -> int:
        """Apply random jitter (0.5x to 1.5x)."""
        jitter_factor = random.uniform(0.5, 1.5)
        return int(delay_ms * jitter_factor)

    async def wait_for_slot(self, source: str) -> None:
        """
        Wait until a request slot is available.

        Applies rate limiting with jitter to avoid thundering herd.
        """
        config = self._get_rate_limit(source)
        min_delay_ms = config["min_delay_ms"]

        # Apply jitter
        delay_ms = self._apply_jitter(min_delay_ms)
        delay_seconds = delay_ms / 1000

        # Check last request time
        last = self._last_request.get(source)
        if last:
            elapsed = (datetime.utcnow() - last).total_seconds()
            if elapsed < delay_seconds:
                wait_time = delay_seconds - elapsed
                logger.debug(
                    "Throttling request",
                    source=source,
                    wait_seconds=wait_time,
                )
                await asyncio.sleep(wait_time)

        # Update last request time
        self._last_request[source] = datetime.utcnow()

    async def wait_with_backoff(self, source: str, attempt: int) -> None:
        """
        Wait with exponential backoff.

        Args:
            source: Source name
            attempt: Attempt number (0-indexed)
        """
        # Base delay with exponential backoff
        base_delay_seconds = min(60 * (2 ** attempt), 900)  # Max 15 minutes

        # Apply jitter
        delay = base_delay_seconds * random.uniform(0.5, 1.5)

        logger.info(
            "Backoff wait",
            source=source,
            attempt=attempt,
            delay_seconds=delay,
        )
        await asyncio.sleep(delay)


# ═══════════════════════════════════════════════════════════════════════════════
# RATE LIMITER (Redis-based for distributed)
# ═══════════════════════════════════════════════════════════════════════════════


class RateLimiter:
    """
    Token bucket rate limiter using Redis.

    For distributed rate limiting across multiple workers.
    """

    def __init__(self, redis_client: Any = None):
        self.redis = redis_client

    async def is_allowed(self, source: str, tokens: int = 1) -> bool:
        """
        Check if request is allowed under rate limit.

        Uses sliding window algorithm.
        """
        if not self.redis:
            # No Redis = always allow (use Throttler for local rate limiting)
            return True

        config = SOURCE_RATE_LIMITS.get(source, DEFAULT_RATE_LIMIT)
        rpm = config["rpm"]

        key = f"ratelimit:{source}"
        now = datetime.utcnow()
        window_start = now - timedelta(minutes=1)

        try:
            # Remove old entries
            await self.redis.zremrangebyscore(key, "-inf", window_start.timestamp())

            # Count current requests
            count = await self.redis.zcard(key)

            if count + tokens <= rpm:
                # Add new request
                await self.redis.zadd(key, {str(now.timestamp()): now.timestamp()})
                await self.redis.expire(key, 120)  # 2 minute TTL
                return True
            else:
                logger.warning("Rate limit exceeded", source=source, current=count, limit=rpm)
                return False

        except Exception as e:
            logger.error("Rate limiter error", error=str(e))
            return True  # Fail open

    async def wait_for_token(self, source: str, max_wait_seconds: int = 60) -> bool:
        """
        Wait for a rate limit token to become available.

        Returns:
            True if token acquired, False if timeout
        """
        start = datetime.utcnow()

        while (datetime.utcnow() - start).total_seconds() < max_wait_seconds:
            if await self.is_allowed(source):
                return True
            await asyncio.sleep(1)

        return False


# ═══════════════════════════════════════════════════════════════════════════════
# ASYNC HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


async def async_random_delay(min_ms: int = 500, max_ms: int = 2000) -> None:
    """Add a random delay (for human-like behavior)."""
    delay = random.randint(min_ms, max_ms)
    await asyncio.sleep(delay / 1000)


async def human_typing_delay() -> None:
    """Add a delay to simulate human typing."""
    await async_random_delay(100, 300)


async def page_load_delay() -> None:
    """Add a delay after page load."""
    await async_random_delay(1000, 3000)


async def between_pages_delay() -> None:
    """Add a longer delay between page navigations (simulates user deciding to go next)."""
    await async_random_delay(3000, 6000)


async def human_scroll(page: Any) -> None:
    """
    Simulate human-like scrolling behavior on a page.
    
    Scrolls down gradually in random increments with pauses,
    then scrolls back up slightly — like a real user reading content.
    """
    try:
        # Get page height
        page_height = await page.evaluate("document.body.scrollHeight")
        viewport_height = await page.evaluate("window.innerHeight")
        
        if page_height <= viewport_height:
            return  # Nothing to scroll
        
        current_position = 0
        scroll_distance = page_height - viewport_height
        
        # Scroll down in 3-6 increments
        num_scrolls = random.randint(3, 6)
        step = scroll_distance / num_scrolls
        
        for i in range(num_scrolls):
            # Random scroll amount (±30% of step)
            scroll_amount = int(step * random.uniform(0.7, 1.3))
            current_position = min(current_position + scroll_amount, scroll_distance)
            
            await page.evaluate(f"window.scrollTo(0, {current_position})")
            
            # Pause between scrolls (simulates reading)
            await asyncio.sleep(random.uniform(0.3, 0.8))
        
        # Scroll to bottom to ensure all content is loaded
        await page.evaluate(f"window.scrollTo(0, {scroll_distance})")
        await asyncio.sleep(random.uniform(0.5, 1.0))
        
        # Scroll back up slightly (natural behavior — user goes back to check something)
        scroll_back = int(scroll_distance * random.uniform(0.1, 0.3))
        await page.evaluate(f"window.scrollTo(0, {scroll_distance - scroll_back})")
        await asyncio.sleep(random.uniform(0.3, 0.6))
        
        # Scroll back to top for extraction
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(random.uniform(0.2, 0.5))
        
    except Exception:
        pass  # Non-critical — don't fail scrape over scroll


async def human_mouse_jitter(page: Any) -> None:
    """
    Simulate random mouse movements in Playwright.
    
    Moves mouse to 2-4 random positions with small pauses.
    """
    try:
        for _ in range(random.randint(2, 4)):
            x = random.randint(100, 900)
            y = random.randint(100, 500)
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.1, 0.3))
    except Exception:
        pass  # Non-critical

