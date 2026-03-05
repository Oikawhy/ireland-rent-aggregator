"""
AGPARS Property.ie Adapter

Full-list pagination scraping for property.ie with Cloudflare stealth.
Uses undetected-chromedriver (UC) to bypass Cloudflare protection.

Note: property.ie uses Cloudflare challenge ("Just a moment...") which
blocks standard Playwright. This adapter requires UC driver, same as Daft.
"""

import re
import asyncio
import random
from typing import Any, Optional

from packages.observability.logger import get_logger
from services.collector.runner import BaseAdapter, RawListing
from services.collector.sanitize import sanitize_text, sanitize_url
from services.collector.throttle import (
    page_load_delay,
    human_scroll,
    human_mouse_jitter,
    between_pages_delay,
)

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# UC / PLAYWRIGHT COMPATIBILITY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


async def uc_get_text(element: Any) -> Optional[str]:
    """Get text from element (UC driver or Playwright compatible)."""
    if element is None:
        return None
    # UC driver uses .text property; Playwright uses .inner_text() coroutine
    if hasattr(element, "text") and isinstance(element.text, str):
        return element.text.strip()
    elif hasattr(element, "inner_text"):
        return await element.inner_text()
    return None


async def uc_get_attr(element: Any, attr: str) -> Optional[str]:
    """Get attribute from element (UC driver or Playwright compatible)."""
    if element is None:
        return None
    result = element.get_attribute(attr)
    if asyncio.iscoroutine(result):
        return await result
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY.IE ADAPTER
# ═══════════════════════════════════════════════════════════════════════════════


class PropertyIeAdapter(BaseAdapter):
    """
    Adapter for Property.ie — uses UC driver to bypass Cloudflare.

    Strategy: Full-list pagination with Cloudflare stealth.
    Cookie consent: Didomi #didomi-notice-agree-button
    Listings: div.search_result
    Pagination: "Next »" link inside div#pages — click first, URL fallback.

    Stealth measures:
      - undetected-chromedriver for Cloudflare bypass
      - Cloudflare challenge detection + wait
      - Click-based pagination (no direct URL navigation after page 1)
      - URL fallback if click fails
      - Human-like scroll, mouse jitter, random delays between pages
      - Ad overlay removal before click attempts
      - 5000 listing safety cap
    """

    BASE_URL = "https://www.property.ie"

    # Use undetected-chromedriver to bypass Cloudflare
    requires_uc_driver = True

    def get_source_name(self) -> str:
        return "property"

    def get_search_url(self, city: str | None = None, county: str | None = None) -> str:
        """Build Property.ie search URL."""
        return f"{self.BASE_URL}/property-to-let/ireland/price_international_rental-onceoff_standard/"

    def _build_page_url(self, base_url: str, page_num: int) -> str:
        """Build paginated URL (fallback).

        Pattern: {base_url}p_{N}/
        Example: /property-to-let/ireland/price_.../p_55/
        """
        if page_num <= 1:
            return base_url
        base = base_url.rstrip("/") + "/"
        return f"{base}p_{page_num}/"

    async def scrape(
        self,
        page: Any,
        city: str | None = None,
        county: str | None = None,
        max_pages: int | None = None,
    ) -> list[RawListing]:
        """Scrape listings from Property.ie with UC driver stealth.

        Primary: click "Next »" link (natural browser navigation)
        Fallback: construct URL and navigate if click fails.
        Safety cap: 5000 listings or max_pages (default 200).
        """
        base_url = self.get_search_url(city, county)
        listings = []
        page_limit = max_pages or 200

        try:
            # ── Page 1: navigate, wait for Cloudflare, handle cookies ──
            await page.goto(base_url, wait_until="domcontentloaded", timeout=45000)

            # Wait for Cloudflare challenge to resolve
            cf_passed = await self._wait_for_cloudflare(page)
            if not cf_passed:
                self.logger.error(
                    "Cloudflare challenge not solved",
                    url=base_url,
                )
                return listings

            await page_load_delay()
            await self._human_pause(2, 4)
            await human_scroll(page)
            await human_mouse_jitter(page)
            await self._remove_ad_overlays(page)
            await self._handle_cookie_consent(page)
            await self._remove_ad_overlays(page)

            page_listings = await self._extract_listings(page)
            listings.extend(page_listings)
            self.logger.info("Page scraped", page=1, found=len(page_listings), total=len(listings))

            if not page_listings:
                self.logger.warning("No listings on page 1, stopping")
                return listings

            # ── Pages 2+: click-based pagination with URL fallback ──
            for page_num in range(2, page_limit + 1):
                # Human-like delay between pages (4-8 seconds — slower for CF sites)
                await self._human_pause(4, 8)

                # Try click-based navigation first (stealthier)
                navigated = await self._click_next_page(page)

                if not navigated:
                    # Fallback: URL-based navigation
                    page_url = self._build_page_url(base_url, page_num)
                    self.logger.debug("Click failed, using URL fallback", page=page_num)
                    try:
                        await page.goto(page_url, wait_until="domcontentloaded", timeout=45000)
                        # Check for Cloudflare on each page navigation
                        cf_ok = await self._wait_for_cloudflare(page, max_wait=30)
                        if not cf_ok:
                            self.logger.warning(
                                "Cloudflare block on pagination, stopping",
                                page=page_num,
                            )
                            break
                    except Exception as nav_err:
                        self.logger.warning(
                            "URL navigation also failed, stopping",
                            page=page_num, error=str(nav_err),
                        )
                        break

                await page_load_delay()
                await self._remove_ad_overlays(page)
                await human_scroll(page)
                await human_mouse_jitter(page)

                page_listings = await self._extract_listings(page)
                listings.extend(page_listings)
                self.logger.info(
                    "Page scraped", page=page_num,
                    found=len(page_listings), total=len(listings),
                )

                # Stop if no listings found (past last page)
                if not page_listings:
                    self.logger.info("No more listings, stopping", last_page=page_num)
                    break

                # Safety cap
                if len(listings) > 10000:
                    self.logger.warning("Reached listing safety cap", count=len(listings))
                    break

        except Exception as e:
            self.logger.error("Scrape failed", error=str(e), city=city, county=county)
            raise

        self.logger.info("Scrape completed", city=city, county=county, total=len(listings))
        return listings

    # ── CLOUDFLARE DETECTION ──

    async def _wait_for_cloudflare(self, page: Any, max_wait: int = 60) -> bool:
        """Wait for Cloudflare challenge to resolve.

        Polls the page title — 'Just a moment...' means CF is still active.
        Returns True if challenge was solved, False if timed out.
        """
        start = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start < max_wait:
            try:
                title = await page.evaluate("document.title")
                title_lower = str(title).lower()

                if "just a moment" not in title_lower and "cloudflare" not in title_lower:
                    self.logger.info(
                        "Cloudflare challenge passed",
                        page_title=str(title)[:80],
                        wait_seconds=round(asyncio.get_event_loop().time() - start, 1),
                    )
                    return True
            except Exception:
                pass

            await asyncio.sleep(2)

        self.logger.warning("Cloudflare challenge timeout", max_wait=max_wait)
        return False

    # ── HUMAN-LIKE DELAYS ──

    async def _human_pause(self, min_sec: float, max_sec: float) -> None:
        """Random pause to simulate human reading/thinking."""
        await asyncio.sleep(random.uniform(min_sec, max_sec))

    # ── CLICK-BASED PAGINATION ──

    async def _click_next_page(self, page: Any) -> bool:
        """Click the 'Next »' pagination link.

        Property.ie pagination: a container (varies by page version) holds
        numbered links and a 'Next »' link. We try:
          1. JS click on 'Next »' link (searches all links on page)
          2. Playwright force click
          3. Return False for URL fallback

        Returns:
            True if successfully navigated to next page.
        """
        try:
            # Remove overlays that might block the click
            await self._remove_ad_overlays(page)

            # Method 1: JS click — search ALL links on the page for "Next" or "»"
            clicked = await page.evaluate('''
                (() => {
                    // Try specific containers first, then fall back to whole document
                    const containers = [
                        document.querySelector("#pages"),
                        document.querySelector("div.pages"),
                        document.querySelector(".paging"),
                        document.querySelector(".pagination"),
                        document,
                    ].filter(Boolean);

                    for (const container of containers) {
                        const links = container.querySelectorAll("a");
                        for (const a of links) {
                            const text = a.textContent.trim();
                            if (text.includes("Next") || text === "»" || text === "Next »") {
                                a.click();
                                return true;
                            }
                        }
                    }
                    return false;
                })()
            ''')

            if clicked:
                # Wait for navigation to complete
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=30000)
                except Exception:
                    await asyncio.sleep(3)

                # Brief CF check on pagination
                await self._wait_for_cloudflare(page, max_wait=15)

                self.logger.debug("Navigated via Next click (JS)")
                return True

            # Method 2: Playwright query + force click (broader selectors)
            next_link = await self._find_next_link(page)
            if next_link:
                try:
                    await next_link.click(force=True)
                    try:
                        await page.wait_for_load_state("domcontentloaded", timeout=30000)
                    except Exception:
                        await asyncio.sleep(3)
                    await self._wait_for_cloudflare(page, max_wait=15)
                    self.logger.debug("Navigated via Next click (Playwright force)")
                    return True
                except Exception as e:
                    self.logger.debug("Playwright force click failed", error=str(e))

        except Exception as e:
            self.logger.debug("Click navigation failed", error=str(e))

        return False

    async def _find_next_link(self, page: Any) -> Any:
        """Find the 'Next »' pagination link element."""
        selectors = [
            '#pages a:has-text("Next")',
            '#pages a:has-text("»")',
            'div.pages a:has-text("Next")',
            '.paging a:has-text("Next")',
        ]
        for selector in selectors:
            try:
                el = await page.query_selector(selector)
                if el:
                    return el
            except Exception:
                continue
        return None

    async def _has_next_page(self, page: Any) -> bool:
        """Check if there's a next page link."""
        link = await self._find_next_link(page)
        return link is not None

    # ── COOKIE CONSENT ──

    async def _handle_cookie_consent(self, page: Any) -> None:
        """Handle Didomi cookie consent popup.

        Button: #didomi-notice-agree-button with text "Accept All"

        Google Ads iframes often overlay the consent button, blocking
        Playwright's native click. We use JS click to bypass.
        """
        await asyncio.sleep(2)

        selectors = [
            "#didomi-notice-agree-button",
            'button[aria-label="Accept All"]',
        ]

        for attempt in range(3):
            for selector in selectors:
                try:
                    # Method 1: JS click (bypasses overlay elements)
                    clicked = await page.evaluate(f'''
                        (() => {{
                            const btn = document.querySelector("{selector}");
                            if (btn) {{ btn.click(); return true; }}
                            return false;
                        }})()
                    ''')
                    if clicked:
                        await asyncio.sleep(1)
                        logger.info("Cookie consent accepted (JS click)",
                                    selector=selector, attempt=attempt + 1)
                        return
                except Exception:
                    pass

                try:
                    # Method 2: Playwright click with force
                    btn = await page.query_selector(selector)
                    if btn:
                        await btn.click(force=True)
                        await asyncio.sleep(1)
                        logger.info("Cookie consent accepted (force click)",
                                    selector=selector, attempt=attempt + 1)
                        return
                except Exception:
                    continue

            if attempt < 2:
                await asyncio.sleep(2)

        logger.debug("Cookie consent button not found (may not be present)")

    # ── AD OVERLAY REMOVAL ──

    async def _remove_ad_overlays(self, page: Any) -> None:
        """Remove Google Ads iframes and sticky overlays that block clicks."""
        try:
            removed = await page.evaluate('''
                (() => {
                    let count = 0;
                    // Remove Google ad iframes
                    document.querySelectorAll("iframe[id^='aswift_'], iframe[title='Advertisement'], ins.adsbygoogle").forEach(el => {
                        el.remove();
                        count++;
                    });
                    // Remove fixed/sticky ad containers
                    document.querySelectorAll("[class*='ad-'], [id*='google_ads'], .ad-container, [data-ad-status]").forEach(el => {
                        const style = window.getComputedStyle(el);
                        if (style.position === 'fixed' || style.position === 'sticky' || el.tagName === 'INS') {
                            el.remove();
                            count++;
                        }
                    });
                    // Remove vignette overlays
                    document.querySelectorAll("[data-vignette-loaded]").forEach(el => {
                        el.remove();
                        count++;
                    });
                    // Remove Cloudflare challenge overlays
                    document.querySelectorAll("#cf-wrapper, .cf-browser-verification").forEach(el => {
                        el.remove();
                        count++;
                    });
                    return count;
                })()
            ''')
            if removed:
                logger.debug("Removed ad overlays", count=removed)
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug("Ad overlay removal failed", error=str(e))

    # ── LISTING EXTRACTION ──

    async def _extract_listings(self, page: Any) -> list[RawListing]:
        """Extract listings from current page."""
        listings = []

        # Container: div.search_result
        cards = await page.query_selector_all("div.search_result")

        if not cards:
            # Debug: log page state
            try:
                title = await page.evaluate("document.title")
                url = await page.evaluate("window.location.href")
                self.logger.warning(
                    "No cards found on page",
                    page_title=str(title)[:100],
                    page_url=str(url)[:200],
                )
            except Exception:
                pass

        for card in cards:
            try:
                listing = await self._parse_card(card)
                if listing:
                    listings.append(listing)
            except Exception as e:
                self.logger.debug("Failed to parse card", error=str(e))

        return listings

    async def _parse_card(self, card: Any) -> RawListing | None:
        """Parse a single property card.

        Structure:
            .sresult_address h2 a  →  title + URL
            .sresult_description h3  →  price (e.g. "€900 monthly")
            .sresult_description h4  →  details (e.g. "1 bedroom..., Apartment to Rent")
            img.thumb  →  property image
            .ber-search-results img  →  BER rating
        """
        # Get link from address: .sresult_address h2 a
        link = await card.query_selector(".sresult_address h2 a")
        if not link:
            link = await card.query_selector("a[href*='/property-to-let/']")
        if not link:
            return None

        href = await uc_get_attr(link, "href")
        if not href:
            return None

        url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
        source_id = self._extract_id_from_url(url)
        if not source_id:
            return None

        # Title = address text
        title = await uc_get_text(link)

        # Price: .sresult_description h3 (e.g. "€900 monthly")
        price_el = await card.query_selector(".sresult_description h3")
        price_text = await uc_get_text(price_el)

        # Details: .sresult_description h4
        details_el = await card.query_selector(".sresult_description h4")
        details_text = await uc_get_text(details_el)

        beds_text = None
        baths_text = None
        if details_text:
            beds_match = re.search(r"(\d+)\s*bedroom", details_text, re.I)
            baths_match = re.search(r"(\d+)\s*bathroom", details_text, re.I)
            beds_text = beds_match.group(1) if beds_match else None
            baths_text = baths_match.group(1) if baths_match else None

        # Location = title
        location_text = title

        # Image: img.thumb
        img_el = await card.query_selector("img.thumb")
        if not img_el:
            img_el = await card.query_selector("img")
        first_photo = await uc_get_attr(img_el, "src")

        return RawListing(
            source=self.get_source_name(),
            source_listing_id=source_id,
            url=sanitize_url(url),
            first_photo_url=first_photo,
            raw_payload={
                "title": sanitize_text(title),
                "price_text": price_text,
                "location_text": location_text,
                "details_text": details_text,
            },
            title=sanitize_text(title),
            price_text=price_text,
            beds_text=beds_text,
            baths_text=baths_text,
            location_text=sanitize_text(location_text),
        )

    def _extract_id_from_url(self, url: str) -> str | None:
        """Extract ID from URL.

        Property.ie pattern: /property-to-let/Name-Address/ID/
        """
        match = re.search(r"/(\d{5,})/", url)
        if match:
            return match.group(1)
        match = re.search(r"/(\d+)/?(?:\?|$)", url)
        return match.group(1) if match else None
