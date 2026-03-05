"""
AGPARS Daft.ie Adapter

Full-list pagination scraping for daft.ie.
Uses https://www.daft.ie/property-for-rent/ireland to get ALL listings.
"""

import re
import asyncio
from typing import Any, Optional

from packages.observability.logger import get_logger
from services.collector.runner import BaseAdapter, RawListing
from services.collector.sanitize import (
    sanitize_text,
    sanitize_url,
)
from services.collector.throttle import page_load_delay, human_scroll, human_mouse_jitter, between_pages_delay
from services.collector.jsonld_extractor import extract_listing_from_html

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS FOR UC/PLAYWRIGHT COMPATIBILITY
# ═══════════════════════════════════════════════════════════════════════════════


async def get_element_text(element: Any) -> Optional[str]:
    """Get text from element (compatible with both Playwright and UC driver)."""
    if element is None:
        return None
    # UC driver uses .text property, Playwright uses .inner_text() method
    if hasattr(element, 'text') and isinstance(element.text, str):
        return element.text
    elif hasattr(element, 'inner_text'):
        return await element.inner_text()
    return None


async def get_element_attr(element: Any, attr: str) -> Optional[str]:
    """Get attribute from element (compatible with both Playwright and UC driver)."""
    if element is None:
        return None
    result = element.get_attribute(attr)
    # Playwright returns coroutine, UC returns value directly
    if asyncio.iscoroutine(result):
        return await result
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# DAFT ADAPTER
# ═══════════════════════════════════════════════════════════════════════════════


class DaftAdapter(BaseAdapter):
    """
    Adapter for Daft.ie (Ireland's largest property site).

    Strategy: Full-list pagination — opens /property-for-rent/ireland
    and paginates through all results, imitating a real user.
    
    Note: Daft.ie uses aggressive Cloudflare protection that requires
    undetected-chromedriver instead of Playwright.
    """

    BASE_URL = "https://www.daft.ie"
    SEARCH_URL = "https://www.daft.ie/property-for-rent/ireland"
    
    # Flag indicating this adapter requires undetected-chromedriver
    requires_uc_driver = True

    def get_source_name(self) -> str:
        return "daft"

    def get_search_url(self, city: str | None = None, county: str | None = None) -> str:
        """Build Daft.ie search URL. Always returns the full Ireland listing."""
        return self.SEARCH_URL

    async def scrape(
        self,
        page: Any,
        city: str | None = None,
        county: str | None = None,
        max_pages: int | None = None,
    ) -> list[RawListing]:
        """Scrape ALL rental listings from Daft.ie by paginating through results."""
        url = self.SEARCH_URL
        listings = []

        try:
            await page.goto(url, wait_until="domcontentloaded")
            await page_load_delay()
            await human_scroll(page)
            await human_mouse_jitter(page)

            # Accept cookies if prompted
            await self._handle_cookie_consent(page)

            # Wait for listings to load
            await self._wait_for_listings(page)

            # Extract listings from current page
            page_listings = await self._extract_listings(page)
            listings.extend(page_listings)

            # Handle pagination
            page_num = 1
            page_limit = max_pages or 200
            while await self._has_next_page(page) and page_num < page_limit:
                page_num += 1
                await self._go_to_next_page(page)
                await between_pages_delay()
                await page_load_delay()
                await human_scroll(page)
                page_listings = await self._extract_listings(page)
                listings.extend(page_listings)

                # Safety limit
                if len(listings) > 5000:
                    self.logger.warning("Reached listing limit", count=len(listings))
                    break

        except Exception as e:
            self.logger.error("Scrape failed", error=str(e))
            raise

        self.logger.info("Scrape completed", pages=page_num, total=len(listings))

        return listings

    async def _handle_cookie_consent(self, page: Any) -> None:
        """Handle cookie consent popup if present - click 'Accept All' button.
        
        Daft.ie uses Didomi consent. The button may take a moment to appear
        and can be in the main document or an overlay.
        """
        import asyncio
        
        # Wait for popup to fully render
        await asyncio.sleep(3)
        
        # Selectors to try in order of priority
        selectors = [
            '#didomi-notice-agree-button',
            'button[aria-label="Accept All"]',
            '#onetrust-accept-btn-handler',
        ]
        
        # Try up to 3 times with increasing waits
        for attempt in range(3):
            for selector in selectors:
                try:
                    btn = await page.query_selector(selector)
                    if btn:
                        # Method 1: Direct click via UCElement
                        if hasattr(btn, '_element'):
                            try:
                                btn._element.click()
                                await asyncio.sleep(1)
                                logger.info("Cookie consent accepted (direct click)",
                                            selector=selector, attempt=attempt + 1)
                                return
                            except Exception:
                                pass
                                
                            # Method 2: JS click fallback
                            try:
                                await page.evaluate(
                                    f'document.querySelector("{selector}").click()'
                                )
                                await asyncio.sleep(1)
                                logger.info("Cookie consent accepted (JS click)",
                                            selector=selector, attempt=attempt + 1)
                                return
                            except Exception:
                                pass
                        else:
                            # Playwright page
                            await btn.click()
                            await asyncio.sleep(1)
                            logger.info("Cookie consent accepted",
                                        selector=selector, attempt=attempt + 1)
                            return
                except Exception:
                    continue
            
            # Wait before retry
            if attempt < 2:
                await asyncio.sleep(2)
        
        logger.debug("Cookie consent button not found (may not be present)")

    async def _wait_for_listings(self, page: Any, timeout: int = 30000) -> None:
        """Wait for listing cards to appear."""
        try:
            await page.wait_for_selector('ul[data-testid="results"]', timeout=timeout)
        except Exception:
            # Try alternative selector
            await page.wait_for_selector('li[data-testid^="result-"]', timeout=timeout)

    async def _extract_listings(self, page: Any) -> list[RawListing]:
        """Extract listing data from the current page."""
        listings = []

        # Get all listing cards – filter by result- prefix to exclude ad containers
        cards = await page.query_selector_all('li[data-testid^="result-"]')
        if not cards:
            # Fallback selectors
            cards = await page.query_selector_all('ul[data-testid="results"] > li')

        # Debug: log what the page looks like
        if not cards:
            try:
                title = await page.evaluate("document.title") if hasattr(page, 'evaluate') else "N/A"
                url = await page.evaluate("window.location.href") if hasattr(page, 'evaluate') else "N/A"
                self.logger.warning("No cards found on page",
                                    page_title=str(title)[:100],
                                    page_url=str(url)[:200])
            except Exception as e:
                self.logger.debug("Debug logging failed", error=str(e))
        else:
            self.logger.info("Found listing cards", count=len(cards))

        for card in cards:
            try:
                listing = await self._parse_card(card)
                if listing:
                    # Enhance with JSON-LD as fallback for missing fields
                    listing = await self._enhance_with_jsonld(listing, card)
                    listings.append(listing)
            except Exception as e:
                self.logger.debug("Failed to parse card", error=str(e))

        return listings

    async def _parse_card(self, card: Any) -> RawListing | None:
        """Parse a single listing card."""
        # Extract URL and ID
        link = await card.query_selector("a[href*='/for-rent/']")
        if not link:
            return None

        href = await get_element_attr(link, "href")
        if not href:
            return None

        url = f"{self.BASE_URL}{href}" if href.startswith("/") else href
        source_id = self._extract_id_from_url(url)
        if not source_id:
            return None

        # Extract title / address (on Daft, address IS the title)
        title_el = await card.query_selector('[data-tracking="srp_address"] p')
        if not title_el:
            title_el = await card.query_selector('p[font-weight="SEMIBOLD"]')
        title = await get_element_text(title_el)

        # Extract price
        price_el = await card.query_selector('[data-tracking="srp_price"] p')
        if not price_el:
            # Sub-unit cards keep price inside srp_units
            price_el = await card.query_selector('[data-tracking="srp_units"] p[font-weight="SEMIBOLD"]')
        price_text = await get_element_text(price_el)

        # Extract location (same as title for Daft)
        location_text = title

        # Extract beds/baths from info strip spans
        # REF: <span data-testid="beds"><span>1 Bed</span></span>
        # REF: <span data-testid="baths"><span>1 Bath</span></span>
        beds_text = None
        baths_text = None

        # Primary: data-testid selectors (stable, per REF/Daft.ie/beds.md)
        beds_el = await card.query_selector('[data-testid="beds"]')
        if beds_el:
            span_text = await get_element_text(beds_el)
            if span_text:
                beds_match = re.search(r"(\d+)\s*Bed", span_text, re.I)
                if beds_match:
                    beds_text = beds_match.group(1)

        baths_el = await card.query_selector('[data-testid="baths"]')
        if baths_el:
            span_text = await get_element_text(baths_el)
            if span_text:
                baths_match = re.search(r"(\d+)\s*Bath", span_text, re.I)
                if baths_match:
                    baths_text = baths_match.group(1)

        # Fallback: class-based selectors (dynamic, may change)
        if not beds_text:
            meta_spans = await card.query_selector_all('.sc-620b3daf-1 span')
            if not meta_spans:
                meta_spans = await card.query_selector_all('[data-tracking="srp_meta"] span')
            for span in meta_spans:
                span_text = await get_element_text(span)
                if span_text:
                    if not beds_text:
                        beds_match = re.search(r"(\d+)\s*Bed", span_text, re.I)
                        if beds_match:
                            beds_text = beds_match.group(1)
                    if not baths_text:
                        baths_match = re.search(r"(\d+)\s*Bath", span_text, re.I)
                        if baths_match:
                            baths_text = baths_match.group(1)

        meta_text = f"{beds_text or ''} Bed {baths_text or ''} Bath".strip()

        # Extract image (first property image, not agent logo)
        img_el = await card.query_selector('[data-testid="imageContainer"] img')
        if not img_el:
            img_el = await card.query_selector('img[alt]')
        first_photo = await get_element_attr(img_el, "src")

        return RawListing(
            source=self.get_source_name(),
            source_listing_id=source_id,
            url=sanitize_url(url),
            first_photo_url=first_photo,
            raw_payload={
                "title": sanitize_text(title),
                "price_text": price_text,
                "location_text": location_text,
                "meta_text": meta_text,
            },
            title=sanitize_text(title),
            price_text=price_text,
            beds_text=beds_text,
            baths_text=baths_text,
            location_text=sanitize_text(location_text),
        )

    async def _enhance_with_jsonld(self, listing: RawListing, card: Any) -> RawListing:
        """
        Enhance listing with JSON-LD data as fallback for missing fields.

        This is used when DOM parsing fails to get price, title, or other fields.
        """
        # Check if key fields are missing
        if listing.title and listing.price_text:
            return listing  # All key fields present, no enhancement needed

        try:
            card_html = await card.inner_html()
            jsonld_data = extract_listing_from_html(card_html)

            if not jsonld_data:
                return listing

            # Fill in missing fields from JSON-LD
            if not listing.title and jsonld_data.get("title"):
                listing = listing._replace(title=sanitize_text(jsonld_data["title"]))

            if not listing.price_text and jsonld_data.get("price_text"):
                listing = listing._replace(price_text=jsonld_data["price_text"])

            if not listing.location_text and jsonld_data.get("location_text"):
                listing = listing._replace(location_text=sanitize_text(jsonld_data["location_text"]))

            self.logger.debug(
                "Enhanced listing with JSON-LD",
                listing_id=listing.source_listing_id,
            )

        except Exception as e:
            self.logger.debug("JSON-LD enhancement failed", error=str(e))

        return listing

    def _extract_id_from_url(self, url: str) -> str | None:
        """Extract listing ID from URL."""
        # Daft URLs: /for-rent/apartment-cityname/12345678
        match = re.search(r"/(\d+)(?:\?|$)", url)
        return match.group(1) if match else None

    async def _has_next_page(self, page: Any) -> bool:
        """Check if there's a next page."""
        next_btn = await page.query_selector('a[data-testid="next-page-link"]')
        return next_btn is not None

    async def _go_to_next_page(self, page: Any) -> None:
        """Navigate to the next page."""
        next_btn = await page.query_selector('a[data-testid="next-page-link"]')
        if next_btn:
            if hasattr(next_btn, '_element'):
                next_btn._element.click()
            else:
                await next_btn.click()
            await page.wait_for_load_state("domcontentloaded")
