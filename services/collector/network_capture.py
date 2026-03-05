"""
AGPARS Network Capture Module

Playwright request interception for API response capture.
"""

import contextlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class CapturedResponse:
    """Captured network response."""

    url: str
    status: int
    content_type: str
    body: str | bytes | None = None
    json_data: dict | list | None = None
    timestamp: float = 0


@dataclass
class CaptureConfig:
    """Configuration for network capture."""

    # URL patterns to capture (regex or substring)
    url_patterns: list[str] = field(default_factory=list)

    # Content types to capture
    content_types: list[str] = field(default_factory=lambda: ["application/json"])

    # Max body size to capture (bytes)
    max_body_size: int = 1024 * 1024  # 1MB

    # Whether to continue the request
    continue_request: bool = True


# ═══════════════════════════════════════════════════════════════════════════════
# NETWORK CAPTURE
# ═══════════════════════════════════════════════════════════════════════════════


class NetworkCapture:
    """
    Captures XHR/Fetch responses using Playwright request interception.

    More reliable than DOM scraping for API-backed listings.
    """

    def __init__(self, config: CaptureConfig | None = None):
        self.config = config or CaptureConfig()
        self.responses: list[CapturedResponse] = []
        self._handlers: list[Callable] = []

    def should_capture(self, url: str, content_type: str) -> bool:
        """Check if a response should be captured."""
        # Check content type
        if self.config.content_types:  # noqa: SIM102
            if not any(ct in content_type for ct in self.config.content_types):
                return False

        # Check URL patterns
        if self.config.url_patterns:
            return any(pattern in url for pattern in self.config.url_patterns)

        return True

    async def start(self, page: Any) -> None:
        """
        Start capturing on a Playwright page.

        Args:
            page: Playwright Page instance
        """
        async def handle_response(response):
            try:
                url = response.url
                content_type = response.headers.get("content-type", "")

                if not self.should_capture(url, content_type):
                    return

                # Get response body
                try:
                    body = await response.body()
                    if len(body) > self.config.max_body_size:
                        logger.debug("Response too large, skipping", url=url, size=len(body))
                        return
                except Exception:
                    body = None

                # Parse JSON if applicable
                json_data = None
                if body and "application/json" in content_type:
                    with contextlib.suppress(json.JSONDecodeError, UnicodeDecodeError):
                        json_data = json.loads(body.decode("utf-8"))

                captured = CapturedResponse(
                    url=url,
                    status=response.status,
                    content_type=content_type,
                    body=body,
                    json_data=json_data,
                )
                self.responses.append(captured)

                logger.debug(
                    "Response captured",
                    url=url[:100],
                    status=response.status,
                    has_json=json_data is not None,
                )

            except Exception as e:
                logger.error("Failed to capture response", error=str(e))

        page.on("response", handle_response)

    def get_json_responses(self) -> list[dict | list]:
        """Get all captured JSON responses."""
        return [r.json_data for r in self.responses if r.json_data is not None]

    def find_response(self, url_pattern: str) -> CapturedResponse | None:
        """Find a captured response by URL pattern."""
        for response in self.responses:
            if url_pattern in response.url:
                return response
        return None

    def find_all_responses(self, url_pattern: str) -> list[CapturedResponse]:
        """Find all captured responses matching URL pattern."""
        return [r for r in self.responses if url_pattern in r.url]

    def clear(self) -> None:
        """Clear captured responses."""
        self.responses.clear()


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE-SPECIFIC PATTERNS
# ═══════════════════════════════════════════════════════════════════════════════


# Known API patterns for Irish rental sites
SOURCE_API_PATTERNS = {
    "daft": [
        "/api/v1/search",
        "/api/listings",
        "gateway/api/search",
    ],
    "rent": [
        "/api/properties",
        "/search/results",
    ],
    "myhome": [
        "/api/search",
        "/propertybrief",
    ],
    "property": [
        "/api/search",
        "/listing-search",
    ],
}


def get_capture_config(source: str) -> CaptureConfig:
    """Get capture configuration for a source."""
    patterns = SOURCE_API_PATTERNS.get(source, [])
    return CaptureConfig(url_patterns=patterns)


# ═══════════════════════════════════════════════════════════════════════════════
# LISTING EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════


def extract_listings_from_json(data: dict | list, source: str) -> list[dict]:
    """
    Extract listings from captured API JSON.

    This is a generic extractor - source-specific logic should
    be implemented in adapter modules.
    """
    listings = []

    # Common patterns for listings in API responses
    if isinstance(data, list):
        listings = data
    elif isinstance(data, dict):
        # Look for common listing array keys
        for key in ["listings", "results", "properties", "items", "data"]:
            if key in data and isinstance(data[key], list):
                listings = data[key]
                break

        # Nested data structure
        if not listings and "data" in data:
            nested = data["data"]
            if isinstance(nested, dict):
                for key in ["listings", "results", "properties"]:
                    if key in nested and isinstance(nested[key], list):
                        listings = nested[key]
                        break

    return listings


async def capture_api_listings(
    page: Any,
    source: str,
    wait_for_selector: str | None = None,
) -> list[dict]:
    """
    Capture listings from API responses after page navigation.

    Args:
        page: Playwright Page
        source: Source name
        wait_for_selector: Optional selector to wait for

    Returns:
        List of raw listing dicts from API
    """
    config = get_capture_config(source)
    capture = NetworkCapture(config)

    await capture.start(page)

    # Wait for content
    if wait_for_selector:
        with contextlib.suppress(Exception):
            await page.wait_for_selector(wait_for_selector, timeout=30000)

    # Wait a bit more for async API calls
    await page.wait_for_timeout(2000)

    # Extract listings from captured responses
    all_listings = []
    for response in capture.responses:
        if response.json_data:
            listings = extract_listings_from_json(response.json_data, source)
            all_listings.extend(listings)

    logger.info(
        "API listings captured",
        source=source,
        responses=len(capture.responses),
        listings=len(all_listings),
    )

    return all_listings
