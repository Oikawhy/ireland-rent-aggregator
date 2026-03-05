"""
AGPARS Collector Runner

Main orchestration for scraping jobs using Playwright browser automation.
"""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from packages.observability.logger import get_logger
from packages.observability.metrics import (
    record_listings_found,
    record_scrape_job,
    PARSE_FAILURES_TOTAL,
    SCRAPE_ERRORS_TOTAL,
)
from packages.storage.listings import upsert_raw_listing

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class RawListing:
    """Raw listing data extracted from a source."""

    source: str
    source_listing_id: str
    url: str
    raw_payload: dict = field(default_factory=dict)
    first_photo_url: str | None = None

    # Parsed fields (for normalization)
    title: str | None = None
    price_text: str | None = None
    beds_text: str | None = None
    baths_text: str | None = None
    property_type_text: str | None = None
    location_text: str | None = None
    description: str | None = None
    furnished_text: str | None = None
    lease_text: str | None = None


@dataclass
class ScrapeResult:
    """Result of a scrape job."""

    source: str
    city: str | None
    county: str | None
    listings: list[RawListing] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0
    success: bool = True


@dataclass
class ScrapeJob:
    """Definition of a scrape job."""

    source: str
    city: str | None = None
    county: str | None = None
    city_id: int | None = None
    job_id: int | None = None
    max_pages: int | None = None  # Limit pagination (None = no limit)


# ═══════════════════════════════════════════════════════════════════════════════
# BASE ADAPTER
# ═══════════════════════════════════════════════════════════════════════════════


class BaseAdapter(ABC):
    """Base class for source-specific scraping adapters."""

    def __init__(self):
        self.logger = get_logger(f"adapter.{self.get_source_name()}")

    @abstractmethod
    def get_source_name(self) -> str:
        """Return the source identifier (e.g., 'daft', 'rent')."""
        pass

    @abstractmethod
    async def scrape(
        self,
        page: Any,  # Playwright Page
        city: str | None = None,
        county: str | None = None,
        max_pages: int | None = None,
    ) -> list[RawListing]:
        """
        Scrape listings from the source.

        Args:
            page: Playwright page instance
            city: Target city name
            county: Target county name
            max_pages: Maximum number of pages to scrape (None = no limit)

        Returns:
            List of RawListing objects
        """
        pass

    def get_search_url(self, city: str | None = None, county: str | None = None) -> str:
        """Build the search URL for the source."""
        raise NotImplementedError

    async def wait_for_listings(self, page: Any, timeout: int = 30000) -> None:  # noqa: B027
        """Wait for listings to load on the page."""
        pass

    async def handle_pagination(self, _page: Any) -> bool:
        """
        Handle pagination if available.

        Returns:
            True if there are more pages, False otherwise
        """
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# COLLECTOR RUNNER
# ═══════════════════════════════════════════════════════════════════════════════


class CollectorRunner:
    """
    Main collector orchestration.

    Manages browser sessions, dispatches to adapters, and saves results.
    
    Supports hybrid browser approach:
    - Playwright for most sites
    - undetected-chromedriver for Cloudflare-protected sites (e.g., Daft.ie)

    Features (Phase 3):
    - SessionManager for cookie persistence per source
    - AnomalyDetector for circuit breaker on volume drops / parse spikes
    """

    def __init__(self, adapters: dict[str, BaseAdapter] | None = None):
        from services.collector.session_manager import SessionManager

        self.adapters = adapters or {}
        self.logger = get_logger(__name__)
        self.session_manager = SessionManager()
        self._browser = None
        self._context = None
        self._uc_driver = None  # For undetected-chromedriver

    def register_adapter(self, adapter: BaseAdapter) -> None:
        """Register a source adapter."""
        self.adapters[adapter.get_source_name()] = adapter

    async def start(self) -> None:
        """Start the browser with full stealth mode and persistent context."""
        from playwright.async_api import async_playwright
        import os

        self._playwright = await async_playwright().start()
        
        # Use persistent context to save cookies between runs
        user_data_dir = os.path.join(os.path.dirname(__file__), ".browser_data")
        os.makedirs(user_data_dir, exist_ok=True)
        
        # Detect Docker environment — use bundled Chromium in headless mode
        in_docker = os.path.exists("/.dockerenv") or os.environ.get("DOCKER", "")
        
        launch_kwargs = dict(
            user_data_dir=user_data_dir,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars",
                "--enable-automation=false",
                "--excludeSwitches=enable-automation",
            ],
            viewport={"width": 1200, "height": 700},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-IE",
            timezone_id="Europe/Dublin",
            geolocation={"latitude": 53.3498, "longitude": -6.2603},
            permissions=["geolocation"],
            extra_http_headers={
                "Accept-Language": "en-IE,en-GB;q=0.9,en;q=0.8",
            },
        )
        
        if in_docker:
            # Docker: use bundled Chromium in headless mode (no Chrome binary)
            launch_kwargs["headless"] = True
            self.logger.info("Docker detected — using headless Chromium")
        else:
            # Local: use real Chrome for better anti-detection
            launch_kwargs["channel"] = "chrome"
            launch_kwargs["headless"] = False
            self.logger.info("Local mode — using Chrome browser")
        
        self._context = await self._playwright.chromium.launch_persistent_context(
            **launch_kwargs
        )
        self._browser = None  # Not used with persistent context

        # === FULL STEALTH SCRIPTS FROM ARCHITECTURE ===

        # Navigator property overrides - fixed for bot detection
        await self._context.add_init_script("""
            // Delete webdriver property completely (more effective than redefine)
            delete Object.getPrototypeOf(navigator).webdriver;

            // Create proper PluginArray that passes instanceof check
            // The key is to use Object.setPrototypeOf on the real plugins object
            const plugins = navigator.plugins;
            const pluginData = [
                {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format'},
                {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: ''},
                {name: 'Native Client', filename: 'internal-nacl-plugin', description: ''},
            ];
            // Override length
            Object.defineProperty(navigator.plugins, 'length', {get: () => 3});

            // Mock languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-IE', 'en-GB', 'en']
            });

            // Mock hardware concurrency (typical desktop)
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 8
            });

            // Mock device memory
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8
            });

            // Mock permissions API to return 'granted'
            const originalQuery = navigator.permissions.query;
            navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({state: Notification.permission}) :
                    originalQuery(parameters)
            );

            // Chrome runtime
            window.chrome = {runtime: {}, loadTimes: function() {}, csi: function() {}};
        """)

        # Canvas fingerprint mitigation
        await self._context.add_init_script("""
            const originalGetContext = HTMLCanvasElement.prototype.getContext;
            HTMLCanvasElement.prototype.getContext = function(type, attributes) {
                const context = originalGetContext.apply(this, arguments);
                if (type === '2d' && context) {
                    const originalGetImageData = context.getImageData;
                    context.getImageData = function() {
                        const imageData = originalGetImageData.apply(this, arguments);
                        for (let i = 0; i < imageData.data.length; i += 4) {
                            imageData.data[i] ^= 1;
                        }
                        return imageData;
                    };
                }
                return context;
            };
        """)

        # WebGL fingerprint mitigation
        await self._context.add_init_script("""
            const getParameterProxyHandler = {
                apply: function(target, thisArg, argumentsList) {
                    const param = argumentsList[0];
                    if (param === 37445) return 'Intel Inc.';
                    if (param === 37446) return 'Intel Iris Plus Graphics 640';
                    return Reflect.apply(target, thisArg, argumentsList);
                }
            };
            if (typeof WebGLRenderingContext !== 'undefined') {
                WebGLRenderingContext.prototype.getParameter = new Proxy(
                    WebGLRenderingContext.prototype.getParameter,
                    getParameterProxyHandler
                );
            }
        """)

        self.logger.info("Browser started with full stealth mode")

    async def start_uc_driver(self) -> None:
        """Start the undetected-chromedriver for Cloudflare-protected sites."""
        from services.collector.uc_driver import UCDriver
        
        if self._uc_driver is None or not self._uc_driver.is_running:
            driver = UCDriver()
            try:
                await driver.start()
            except Exception as e:
                self.logger.error("UC Driver failed to start", error=str(e))
                try:
                    await driver.stop()
                except Exception:
                    pass
                self._uc_driver = None
                raise
            self._uc_driver = driver
            self.logger.info("UC Driver started for Cloudflare-protected sites")

    async def stop(self) -> None:
        """Stop all browsers."""
        # Stop Playwright
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if hasattr(self, "_playwright"):
            await self._playwright.stop()
        
        # Stop UC Driver
        if self._uc_driver:
            await self._uc_driver.stop()
            self._uc_driver = None
        
        self.logger.info("Browser stopped")

    async def run_job(self, job: ScrapeJob) -> ScrapeResult:
        """
        Execute a single scrape job.

        Args:
            job: The job to execute

        Returns:
            ScrapeResult with listings or errors
        """
        from services.collector.circuit_breaker import check_circuit, record_failure, record_success
        from services.collector.throttle import Throttler

        start_time = datetime.utcnow()

        # Check circuit breaker
        circuit_state = check_circuit(job.source)
        if circuit_state == "open":
            self.logger.warning("Circuit OPEN, skipping job", source=job.source)
            return ScrapeResult(
                source=job.source,
                city=job.city,
                county=job.county,
                success=False,
                errors=["Circuit breaker OPEN"],
            )

        # Get adapter
        adapter = self.adapters.get(job.source)
        if not adapter:
            self.logger.error("No adapter for source", source=job.source)
            return ScrapeResult(
                source=job.source,
                city=job.city,
                county=job.county,
                success=False,
                errors=[f"No adapter for source: {job.source}"],
            )

        # Apply throttling
        throttler = Throttler()
        await throttler.wait_for_slot(job.source)

        # Create page - use UC driver for adapters that require it
        use_uc = getattr(adapter, 'requires_uc_driver', False)
        
        if use_uc:
            # Start UC driver if not already running
            if self._uc_driver is None:
                await self.start_uc_driver()
            page = await self._uc_driver.new_page()
            self.logger.info("Using UC driver for scrape", source=job.source)
        else:
            page = await self._context.new_page()
            # Restore session cookies before scrape (only for Playwright)
            await self.session_manager.apply_cookies_to_context(self._context, job.source)

        try:
            # Execute scrape (with Cloudflare retry for UC driver adapters)
            listings = await adapter.scrape(page, job.city, job.county, job.max_pages)

            # Cloudflare retry: if UC driver adapter returns 0 listings,
            # the challenge page likely wasn't bypassed. Retry with cooldown.
            if use_uc and len(listings) == 0:
                import random
                max_retries = 2
                for retry in range(1, max_retries + 1):
                    cooldown = random.randint(240, 360)  # 4-6 min
                    self.logger.warning(
                        "UC adapter returned 0 listings (Cloudflare?), retrying",
                        source=job.source, retry=retry, max_retries=max_retries,
                        cooldown_sec=cooldown,
                    )
                    await asyncio.sleep(cooldown)

                    # Close old page and create fresh one
                    try:
                        # Restart UC driver for a clean session
                        await self._uc_driver.stop()
                        self._uc_driver = None
                        await self.start_uc_driver()
                        page = await self._uc_driver.new_page()
                    except Exception as restart_err:
                        self.logger.error(
                            "UC driver restart failed",
                            error=str(restart_err), retry=retry,
                        )
                        break

                    listings = await adapter.scrape(page, job.city, job.county, job.max_pages)
                    if len(listings) > 0:
                        self.logger.info(
                            "Cloudflare retry succeeded",
                            source=job.source, retry=retry, found=len(listings),
                        )
                        break
                    self.logger.warning(
                        "Cloudflare retry still 0 listings",
                        source=job.source, retry=retry,
                    )

            # Calculate duration
            duration = (datetime.utcnow() - start_time).total_seconds()

            # Parse failures count (estimate from listings without critical fields)
            parse_failures = sum(
                1 for lst in listings
                if not lst.title and not lst.price_text
            )
            if parse_failures > 0:
                PARSE_FAILURES_TOTAL.labels(source=job.source, field="title+price").inc(parse_failures)

            # Record success
            record_success(job.source)

            # ── Prometheus metrics ──
            record_scrape_job(job.source, "success", duration)
            record_listings_found(job.source, job.city or "all", len(listings))

            # Save session cookies after successful scrape
            if use_uc and self._uc_driver and hasattr(page, 'save_cookies_to_store'):
                # Only save cookies if we got listings (don't overwrite good cookies
                # with broken ones from a failed Cloudflare bypass)
                if len(listings) > 0:
                    await page.save_cookies_to_store(job.source)
                else:
                    self.logger.warning(
                        "Skipping cookie save — 0 listings (preserving existing cookies)",
                        source=job.source,
                    )
            else:
                await self.session_manager.save_cookies_from_context(
                    self._context, job.source
                )
            self.session_manager.record_success(job.source)

            # Anomaly detection (may open circuit breaker)
            from services.collector.anomaly_detection import detect_and_handle_anomaly

            anomaly = detect_and_handle_anomaly(
                source=job.source,
                listings_found=len(listings),
                parse_failures=parse_failures,
                duration_seconds=duration,
            )
            if anomaly:
                self.logger.warning(
                    "Anomaly detected",
                    source=job.source,
                    anomaly_type=anomaly.anomaly_type,
                    severity=anomaly.severity,
                    should_open_circuit=anomaly.should_open_circuit,
                )

            # Save listings
            saved_count = 0
            for listing in listings:
                try:
                    upsert_raw_listing(
                        source=listing.source,
                        source_listing_id=listing.source_listing_id,
                        url=listing.url,
                        raw_payload=listing.raw_payload,
                        first_photo_url=listing.first_photo_url,
                        title=listing.title,
                        price_text=listing.price_text,
                        beds_text=listing.beds_text,
                        baths_text=listing.baths_text,
                        property_type_text=listing.property_type_text,
                        location_text=listing.location_text,
                        description=listing.description,
                    )
                    saved_count += 1
                except Exception as e:
                    self.logger.error("Failed to save listing", error=str(e))

            self.logger.info(
                "Job completed",
                source=job.source,
                city=job.city,
                found=len(listings),
                saved=saved_count,
                duration=duration,
            )

            return ScrapeResult(
                source=job.source,
                city=job.city,
                county=job.county,
                listings=listings,
                duration_seconds=duration,
                success=True,
            )

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = str(e)

            # Record failure
            record_failure(job.source, error_msg)

            # ── Prometheus metrics ──
            record_scrape_job(job.source, "failure", duration)
            SCRAPE_ERRORS_TOTAL.labels(source=job.source, reason=type(e).__name__).inc()

            # Record session failure (may trigger rotation)
            should_rotate = self.session_manager.record_failure(job.source)
            if should_rotate:
                self.logger.info("Session rotated after failures", source=job.source)

            self.logger.error(
                "Job failed",
                source=job.source,
                city=job.city,
                error=error_msg,
            )

            return ScrapeResult(
                source=job.source,
                city=job.city,
                county=job.county,
                success=False,
                errors=[error_msg],
                duration_seconds=duration,
            )

        finally:
            # Only close Playwright pages (UC driver uses single-page model)
            if not use_uc:
                await page.close()

    async def run_jobs(self, jobs: list[ScrapeJob]) -> list[ScrapeResult]:
        """Run multiple jobs sequentially with random delays between sources.

        Adds a 1–5 minute randomized pause between jobs to avoid
        predictable request patterns that anti-bot systems detect.
        """
        import random

        results = []
        for i, job in enumerate(jobs):
            result = await self.run_job(job)
            results.append(result)

            # Random delay between jobs (skip after the last one)
            if i < len(jobs) - 1:
                delay_sec = random.randint(60, 300)  # 1–5 minutes
                logger.info(
                    "Inter-job delay (anti-bot)",
                    delay_min=round(delay_sec / 60, 1),
                    next_source=jobs[i + 1].source,
                )
                await asyncio.sleep(delay_sec)

        return results


# ═══════════════════════════════════════════════════════════════════════════════
# FACTORY
# ═══════════════════════════════════════════════════════════════════════════════


def create_collector() -> CollectorRunner:
    """Create a collector with all registered adapters."""
    from services.collector.adapters.daft import DaftAdapter
    from services.collector.adapters.dng import DngAdapter
    from services.collector.adapters.myhome import MyHomeAdapter
    from services.collector.adapters.property_ie import PropertyIeAdapter
    from services.collector.adapters.rent import RentAdapter
    from services.collector.adapters.sherryfitz import SherryFitzAdapter

    runner = CollectorRunner()
    runner.register_adapter(DaftAdapter())
    runner.register_adapter(RentAdapter())
    runner.register_adapter(MyHomeAdapter())
    runner.register_adapter(PropertyIeAdapter())
    runner.register_adapter(SherryFitzAdapter())
    runner.register_adapter(DngAdapter())

    return runner


async def run_single_source(source: str, city: str | None = None, county: str | None = None) -> ScrapeResult:
    """Convenience function to run a single source scrape."""
    runner = create_collector()
    await runner.start()
    try:
        job = ScrapeJob(source=source, city=city, county=county)
        return await runner.run_job(job)
    finally:
        await runner.stop()
