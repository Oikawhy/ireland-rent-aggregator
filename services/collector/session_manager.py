"""
AGPARS Session Manager Module

Playwright browser session and cookie management.
"""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from services.collector.session_store import SessionStore

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════


DEFAULT_COOKIE_LIFETIME_HOURS = 72
DEFAULT_HEALTH_CHECK_INTERVAL_MINUTES = 30
DEFAULT_FORCE_ROTATION_AFTER_FAILURES = 3


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION MANAGER
# ═══════════════════════════════════════════════════════════════════════════════


class SessionManager:
    """
    Manages Playwright browser sessions and cookies.

    Features:
    - Cookie persistence per source
    - Session health checks
    - Automatic rotation on failures
    """

    def __init__(
        self,
        session_store: "SessionStore | None" = None,
        cookie_lifetime_hours: int = DEFAULT_COOKIE_LIFETIME_HOURS,
    ):
        from services.collector.session_store import SessionStore

        self.store = session_store or SessionStore()
        self.cookie_lifetime_hours = cookie_lifetime_hours
        self._failure_counts: dict[str, int] = {}
        self._last_health_check: dict[str, datetime] = {}

    async def get_cookies(self, source: str) -> list[dict] | None:
        """
        Get stored cookies for a source.

        Returns:
            List of cookie dicts or None if expired/not found
        """
        session_data = self.store.load_session(source)
        if not session_data:
            return None

        # Check if expired
        expires_at = session_data.get("expires_at")
        if expires_at and datetime.fromisoformat(expires_at) < datetime.utcnow():
            logger.info("Session expired, will rotate", source=source)
            self.store.invalidate_session(source)
            return None

        return session_data.get("cookies", [])

    async def save_cookies(self, source: str, cookies: list[dict]) -> None:
        """Save cookies for a source."""
        expires_at = datetime.utcnow() + timedelta(hours=self.cookie_lifetime_hours)

        self.store.save_session(
            source=source,
            data={
                "cookies": cookies,
                "created_at": datetime.utcnow().isoformat(),
                "expires_at": expires_at.isoformat(),
            },
            ttl_hours=self.cookie_lifetime_hours,
        )

        # Reset failure count
        self._failure_counts[source] = 0
        logger.info("Cookies saved", source=source, expires_at=expires_at.isoformat())

    async def apply_cookies_to_context(
        self,
        context: Any,  # Playwright BrowserContext
        source: str,
    ) -> bool:
        """
        Apply stored cookies to a browser context.

        Returns:
            True if cookies were applied
        """
        cookies = await self.get_cookies(source)
        if cookies:
            try:
                await context.add_cookies(cookies)
                logger.debug("Cookies applied to context", source=source, count=len(cookies))
                return True
            except Exception as e:
                logger.error("Failed to apply cookies", source=source, error=str(e))

        return False

    async def save_cookies_from_context(
        self,
        context: Any,  # Playwright BrowserContext
        source: str,
    ) -> None:
        """Save cookies from a browser context."""
        try:
            cookies = await context.cookies()
            await self.save_cookies(source, cookies)
        except Exception as e:
            logger.error("Failed to save cookies from context", source=source, error=str(e))

    async def check_session_health(
        self,
        page: Any,  # Playwright Page
        source: str,
    ) -> bool:
        """
        Check if the current session is healthy.

        Override in source-specific managers for custom health checks.

        Returns:
            True if session is healthy
        """
        # Default: check if we got a valid page (not error page)
        try:
            # Simple check - page title exists
            title = await page.title()
            if title and len(title) > 0:
                self._last_health_check[source] = datetime.utcnow()
                return True
        except Exception as e:
            logger.warning("Session health check failed", source=source, error=str(e))

        return False

    def should_check_health(self, source: str) -> bool:
        """Check if a health check is needed."""
        last_check = self._last_health_check.get(source)
        if not last_check:
            return True

        elapsed = datetime.utcnow() - last_check
        return elapsed.total_seconds() > (DEFAULT_HEALTH_CHECK_INTERVAL_MINUTES * 60)

    def record_failure(self, source: str) -> bool:
        """
        Record a session failure.

        Returns:
            True if session should be rotated
        """
        self._failure_counts[source] = self._failure_counts.get(source, 0) + 1
        failures = self._failure_counts[source]

        logger.warning("Session failure recorded", source=source, failures=failures)

        if failures >= DEFAULT_FORCE_ROTATION_AFTER_FAILURES:
            logger.warning("Session rotation triggered", source=source)
            self.store.invalidate_session(source)
            self._failure_counts[source] = 0
            return True

        return False

    def record_success(self, source: str) -> None:
        """Record a successful request, reset failure count."""
        self._failure_counts[source] = 0

    async def rotate_session(self, source: str) -> None:
        """Force session rotation."""
        self.store.invalidate_session(source)
        self._failure_counts[source] = 0
        logger.info("Session rotated", source=source)


# ═══════════════════════════════════════════════════════════════════════════════
# CONTEXT FACTORY
# ═══════════════════════════════════════════════════════════════════════════════


async def create_browser_context(
    browser: Any,  # Playwright Browser
    source: str,
    session_manager: SessionManager | None = None,
) -> Any:
    """
    Create a browser context with optional session restoration.

    Args:
        browser: Playwright browser instance
        source: Source name
        session_manager: Optional session manager for cookie persistence

    Returns:
        Playwright BrowserContext
    """
    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        locale="en-IE",
        timezone_id="Europe/Dublin",
    )

    # Apply stored cookies
    if session_manager:
        await session_manager.apply_cookies_to_context(context, source)

    return context
