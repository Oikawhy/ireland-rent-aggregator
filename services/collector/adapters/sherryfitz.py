"""
AGPARS SherryFitzGerald Adapter

Pagination-based scraping for sherryfitz.ie
"""

import re
from typing import Any

from packages.observability.logger import get_logger
from services.collector.runner import BaseAdapter, RawListing
from services.collector.sanitize import sanitize_text, sanitize_url
from services.collector.throttle import page_load_delay, human_scroll, human_mouse_jitter, between_pages_delay

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# SHERRYFITZ ADAPTER
# ═══════════════════════════════════════════════════════════════════════════════


class SherryFitzAdapter(BaseAdapter):
    """
    Adapter for SherryFitzGerald (sherryfitz.ie)

    Strategy: Pagination with Next arrow button.
    Listings: div.property-card-container (REF/Sherry FitzGerald/1.md)
    Pagination: a.pagination-arrow.pagination-next (REF/Sherry FitzGerald/2.md)
    """

    BASE_URL = "https://www.sherryfitz.ie"

    def get_source_name(self) -> str:
        return "sherryfitz"

    def get_search_url(self, city: str | None = None, county: str | None = None) -> str:
        """Build SherryFitz search URL."""
        return f"{self.BASE_URL}/properties/search?type=rent&sort_by=created_at&sort_order=desc"

    async def scrape(
        self,
        page: Any,
        city: str | None = None,
        county: str | None = None,
        max_pages: int | None = None,
    ) -> list[RawListing]:
        """Scrape listings from SherryFitz."""
        url = self.get_search_url(city, county)
        listings = []

        try:
            await page.goto(url, wait_until="domcontentloaded")
            await page_load_delay()

            # Wait for property cards to render (JS-heavy page)
            try:
                await page.wait_for_selector("div.property-card-container", timeout=15000)
            except Exception:
                # Fallback: wait extra time for JS rendering
                await page.wait_for_timeout(5000)

            await human_scroll(page)
            await human_mouse_jitter(page)

            # Handle cookies
            await self._handle_cookie_consent(page)

            # Extract listings from all pages
            page_num = 1
            page_limit = max_pages or 200
            while page_num <= page_limit:  # Safety limit
                page_listings = await self._extract_listings(page)
                listings.extend(page_listings)

                if not await self._has_next_page(page):
                    break

                page_num += 1
                await between_pages_delay()
                await self._go_to_next_page(page)
                await page_load_delay()

                # Wait for new cards to render after pagination
                try:
                    await page.wait_for_selector("div.property-card-container", timeout=10000)
                except Exception:
                    await page.wait_for_timeout(3000)
                await human_scroll(page)

        except Exception as e:
            self.logger.error("Scrape failed", error=str(e), city=city, county=county)
            raise

        self.logger.info("Scrape completed", city=city, county=county, total=len(listings))
        return listings

    async def _handle_cookie_consent(self, page: Any) -> None:
        """Handle cookie consent."""
        try:
            consent_btn = await page.query_selector("#onetrust-accept-btn-handler")
            if not consent_btn:
                consent_btn = await page.query_selector('button:has-text("Accept")')
            if consent_btn:
                await consent_btn.click()
                await page.wait_for_timeout(500)
        except Exception:
            pass

    async def _extract_listings(self, page: Any) -> list[RawListing]:
        """Extract all listings (REF/Sherry FitzGerald/1.md)."""
        listings = []

        # Container: div.property-card-container
        cards = await page.query_selector_all("div.property-card-container")
        if not cards:
            cards = await page.query_selector_all("div.property-card")

        for card in cards:
            try:
                listing = await self._parse_card(card)
                if listing:
                    listings.append(listing)
            except Exception as e:
                self.logger.debug("Failed to parse card", error=str(e))

        return listings

    async def _parse_card(self, card: Any) -> RawListing | None:
        """Parse a property card (REF/Sherry FitzGerald/1.md structure).
        
        Structure:
            .property-card-info-price  →  price (e.g. "€1,500 per month")
            .property-card-address span  →  address parts joined
            .property-card-stat  →  beds/baths (e.g. "4 beds", "1 baths")
            img.property-thumbnail  →  image
            a.property-card-image-link[href]  →  URL
            div[data-id]  →  listing ID
        """
        # Get listing ID from data-id attribute
        data_id = await card.get_attribute("data-id")

        # Get link: a.property-card-image-link or a.cta-link
        link = await card.query_selector("a.property-card-image-link")
        if not link:
            link = await card.query_selector("a.cta-link")
        if not link:
            link = await card.query_selector('a[href*="/rent/"]')
        if not link:
            return None

        href = await link.get_attribute("href")
        if not href:
            return None

        url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
        source_id = data_id or self._extract_id_from_url(url)
        if not source_id:
            return None

        # Price: .property-card-info-price (e.g. "€1,500 per month")
        price_el = await card.query_selector(".property-card-info-price")
        price_text = await price_el.inner_text() if price_el else None

        # Address: .property-card-address span elements
        address_spans = await card.query_selector_all(".property-card-address span")
        address_parts = []
        for span in address_spans:
            text = await span.inner_text()
            if text and text.strip():
                address_parts.append(text.strip())
        location_text = ", ".join(address_parts) if address_parts else None

        # Title = address
        title = location_text

        # Beds/Baths from .property-card-stat elements
        beds_text = None
        baths_text = None
        stats = await card.query_selector_all(".property-card-stat")
        for stat in stats:
            text = await stat.inner_text()
            if text:
                beds_match = re.search(r"(\d+)\s*bed", text, re.I)
                baths_match = re.search(r"(\d+)\s*bath", text, re.I)
                if beds_match:
                    beds_text = beds_match.group(1)
                if baths_match:
                    baths_text = baths_match.group(1)

        # Image: img.property-thumbnail
        img_el = await card.query_selector("img.property-thumbnail")
        if not img_el:
            img_el = await card.query_selector("img")
        first_photo = await img_el.get_attribute("src") if img_el else None
        if first_photo and not first_photo.startswith("http"):
            first_photo = f"{self.BASE_URL}{first_photo}"

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
        
        SherryFitz uses slug-based URLs without numeric IDs.
        Use the last path segment as identifier.
        """
        # Try to get a slug from the URL path
        match = re.search(r"/rent/[^/]+/[^/]+/([^/?]+)$", url)
        if match:
            return match.group(1)
        # Fallback: last path segment
        match = re.search(r"/([^/?]+)/?(?:\?|$)", url)
        return match.group(1) if match else None

    async def _has_next_page(self, page: Any) -> bool:
        """Check for next page (REF/Sherry FitzGerald/2.md).
        
        Next button: a.pagination-arrow.pagination-next
        When no more pages, this element disappears.
        """
        next_btn = await page.query_selector("a.pagination-arrow.pagination-next")
        return next_btn is not None

    async def _go_to_next_page(self, page: Any) -> None:
        """Navigate to next page by clicking pagination arrow."""
        next_btn = await page.query_selector("a.pagination-arrow.pagination-next")
        if next_btn:
            await next_btn.click()
            await page.wait_for_load_state("domcontentloaded")
