"""
AGPARS DNG Adapter

Full-list pagination with "Load More" button for dng.ie
"""

import re
from typing import Any

from packages.observability.logger import get_logger
from services.collector.runner import BaseAdapter, RawListing
from services.collector.sanitize import sanitize_text, sanitize_url
from services.collector.throttle import page_load_delay, human_scroll, human_mouse_jitter, between_pages_delay

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DNG ADAPTER
# ═══════════════════════════════════════════════════════════════════════════════


class DngAdapter(BaseAdapter):
    """
    Adapter for DNG (dng.ie)

    Strategy: Page-based pagination with /page-N/ suffix.
    URL: https://www.dng.ie/property/to-rent/in-ireland/
    Listings: div.property-card (REF/DNG/1.md)
    Pagination: /page-2/, /page-3/, etc.
    """

    BASE_URL = "https://www.dng.ie"

    def get_source_name(self) -> str:
        return "dng"

    def get_search_url(self, city: str | None = None, county: str | None = None, page: int = 1) -> str:
        """Build DNG search URL with page-based pagination."""
        base = f"{self.BASE_URL}/property/to-rent/in-ireland/"
        if page > 1:
            return f"{base}page-{page}/"
        return base

    async def scrape(
        self,
        page: Any,
        city: str | None = None,
        county: str | None = None,
        max_pages: int | None = None,
    ) -> list[RawListing]:
        """Scrape listings from DNG using page-based pagination."""
        listings = []
        page_limit = max_pages or 200

        try:
            for page_num in range(1, page_limit + 1):
                url = self.get_search_url(city, county, page=page_num)
                self.logger.info("Navigating to page", page=page_num, url=url)

                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page_load_delay()
                await human_scroll(page)
                await human_mouse_jitter(page)

                # Wait for property cards to render
                try:
                    await page.wait_for_selector("div.property-card", timeout=15000)
                except Exception:
                    await page.wait_for_timeout(5000)

                # Handle cookies (only on first page)
                if page_num == 1:
                    await self._handle_cookie_consent(page)

                # Extract listings from this page
                page_listings = await self._extract_listings(page)
                if not page_listings:
                    self.logger.info("No listings on page, stopping", page=page_num)
                    break

                listings.extend(page_listings)
                self.logger.info("Page scraped", page=page_num, found=len(page_listings))

                # Delay between pages
                await between_pages_delay()

        except Exception as e:
            self.logger.error("Scrape failed", error=str(e), city=city, county=county)
            raise

        self.logger.info("Scrape completed", city=city, county=county, total=len(listings))
        return listings

    async def _handle_cookie_consent(self, page: Any) -> None:
        """Handle cookie consent."""
        try:
            consent_btn = await page.query_selector("#CybotCookiebotDialogBodyLevelButtonLevelAcceptAll")
            if not consent_btn:
                consent_btn = await page.query_selector('button:has-text("Accept All")')
            if not consent_btn:
                consent_btn = await page.query_selector("#onetrust-accept-btn-handler")
            if consent_btn:
                await consent_btn.click()
                await page.wait_for_timeout(500)
        except Exception:
            pass

    async def _extract_listings(self, page: Any) -> list[RawListing]:
        """Extract all listings (REF/DNG/1.md)."""
        listings = []

        # Container: div.property-card
        cards = await page.query_selector_all("div.property-card")
        
        # Debug: log what the page looks like
        if not cards:
            try:
                title = await page.evaluate("document.title") if hasattr(page, 'evaluate') else "N/A"
                url = await page.evaluate("window.location.href") if hasattr(page, 'evaluate') else "N/A"
                self.logger.warning("No cards found on page",
                                    page_title=str(title)[:100],
                                    page_url=str(url)[:200])
                # Try alternative selectors
                alt_cards = await page.query_selector_all(".property-card-wrapper, [class*='property'], .search-result")
                self.logger.debug("Alternative selector count", count=len(alt_cards))
            except Exception as e:
                self.logger.debug("Debug logging failed", error=str(e))
        else:
            self.logger.info("Found listing cards", count=len(cards))

        for card in cards:
            try:
                listing = await self._parse_card(card)
                if listing:
                    listings.append(listing)
            except Exception as e:
                self.logger.debug("Failed to parse card", error=str(e))

        return listings

    async def _parse_card(self, card: Any) -> RawListing | None:
        """Parse a property card (REF/DNG/1.md structure).
        
        Structure:
            .property-name a  →  title + URL
            .property-price  →  price (e.g. "€2,500 pcm")
            .bedroom-count  →  beds (icon + number)
            .bathroom-count  →  baths (icon + number)
            .property-img a img  →  image
            .property-desc-content  →  description
        """
        # Get link: .property-name a
        link = await card.query_selector(".property-name a")
        if not link:
            link = await card.query_selector(".property-img a")
        if not link:
            return None

        href = await link.get_attribute("href")
        if not href:
            return None

        url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
        source_id = self._extract_id_from_url(url)
        if not source_id:
            return None

        # Title: .property-name a text
        title_el = await card.query_selector(".property-name a")
        title = await title_el.inner_text() if title_el else None

        # Price: .property-price (e.g. "€2,500 pcm")
        price_el = await card.query_selector(".property-price")
        price_text = await price_el.inner_text() if price_el else None

        # Location = title (address in title)
        location_text = title

        # Beds: .bedroom-count text (e.g. icon + "3")
        beds_text = None
        beds_el = await card.query_selector(".bedroom-count")
        if beds_el:
            text = await beds_el.inner_text()
            match = re.search(r"(\d+)", text)
            beds_text = match.group(1) if match else None

        # Baths: .bathroom-count text
        baths_text = None
        baths_el = await card.query_selector(".bathroom-count")
        if baths_el:
            text = await baths_el.inner_text()
            match = re.search(r"(\d+)", text)
            baths_text = match.group(1) if match else None

        # Image: .property-img a img
        img_el = await card.query_selector(".property-img img")
        if not img_el:
            img_el = await card.query_selector("img")
        first_photo = await img_el.get_attribute("src") if img_el else None

        return RawListing(
            source=self.get_source_name(),
            source_listing_id=source_id,
            url=sanitize_url(url),
            first_photo_url=first_photo,
            raw_payload={
                "title": sanitize_text(title),
                "price_text": price_text,
                "location_text": location_text,
            },
            title=sanitize_text(title),
            price_text=price_text,
            beds_text=beds_text,
            baths_text=baths_text,
            location_text=sanitize_text(location_text),
        )

    def _extract_id_from_url(self, url: str) -> str | None:
        """Extract ID from URL.
        
        DNG URL pattern: /property-to-rent/description-HEX_ID/
        The hex ID is in the last path segment (e.g. 66fe733ffdf54dc1eb2931fe)
        """
        match = re.search(r"-([0-9a-f]{24})/?(?:\?|$)", url)
        if match:
            return match.group(1)
        # Fallback: numeric ID
        match = re.search(r"/(\d+)/?(?:\?|$)", url)
        return match.group(1) if match else None
