"""
AGPARS MyHome.ie Adapter

Multi-city search scraping for myhome.ie
"""

import re
from typing import Any

from packages.observability.logger import get_logger
from services.collector.runner import BaseAdapter, RawListing
from services.collector.sanitize import sanitize_text, sanitize_url
from services.collector.throttle import page_load_delay, human_scroll, human_mouse_jitter, between_pages_delay
from services.collector.jsonld_extractor import extract_listing_from_html

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# MYHOME.IE ADAPTER
# ═══════════════════════════════════════════════════════════════════════════════


class MyHomeAdapter(BaseAdapter):
    """
    Adapter for MyHome.ie

    Strategy: Multi-city search with URL parameters + pagination.
    Cookie consent: OneTrust #onetrust-accept-btn-handler (REF/MyHome.ie/1.md)
    Listings: app-standard-results-card / div.card.standard-card (REF/MyHome.ie/2.md)
    Pagination: ul.ngx-pagination with li.pagination-next (REF/MyHome.ie/3.md)
    """

    BASE_URL = "https://www.myhome.ie"

    def get_source_name(self) -> str:
        return "myhome"

    def get_search_url(self, city: str | None = None, county: str | None = None) -> str:
        """Build MyHome.ie search URL."""
        # Use Ireland-wide rental listings
        return f"{self.BASE_URL}/rentals/ireland/property-to-rent"

    async def scrape(
        self,
        page: Any,
        city: str | None = None,
        county: str | None = None,
        max_pages: int | None = None,
    ) -> list[RawListing]:
        """Scrape listings from MyHome.ie."""
        url = self.get_search_url(city, county)
        listings = []

        try:
            await page.goto(url, wait_until="domcontentloaded")
            await page_load_delay()
            await human_scroll(page)
            await human_mouse_jitter(page)

            # Dismiss any modal overlays first (MyHome shows alert modals)
            await self._dismiss_modals(page)

            # Handle cookie consent (OneTrust)
            await self._handle_cookie_consent(page)

            # Dismiss modals again (may reappear after consent click)
            await self._dismiss_modals(page)

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
                await self._dismiss_modals(page)
                page_listings = await self._extract_listings(page)
                listings.extend(page_listings)

        except Exception as e:
            self.logger.error("Scrape failed", error=str(e), city=city, county=county)
            raise

        self.logger.info("Scrape completed", city=city, county=county, total=len(listings))
        return listings

    async def _dismiss_modals(self, page: Any) -> None:
        """Remove any modal overlays that block interaction.

        MyHome.ie shows <app-mh-modal> "We will let you know if properties come up"
        which intercepts pointer events and causes click timeouts.
        """
        import asyncio
        try:
            removed = await page.evaluate('''
                (() => {
                    let count = 0;
                    // Remove Angular modal components
                    document.querySelectorAll("app-mh-modal, .MhModal, .modal-backdrop, .cdk-overlay-container").forEach(el => {
                        el.remove();
                        count++;
                    });
                    // Remove any full-screen overlays blocking clicks
                    document.querySelectorAll("[class*='overlay'], [class*='modal']").forEach(el => {
                        const style = window.getComputedStyle(el);
                        if (style.position === 'fixed' && parseFloat(style.zIndex) > 999) {
                            el.remove();
                            count++;
                        }
                    });
                    return count;
                })()
            ''')
            if removed:
                logger.info("Dismissed modal overlays", count=removed)
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug("Modal dismissal failed", error=str(e))

    async def _handle_cookie_consent(self, page: Any) -> None:
        """Handle OneTrust cookie consent popup (REF/MyHome.ie/1.md).
        
        Button: #onetrust-accept-btn-handler with text "I ACCEPT"
        Uses JS click to bypass any remaining overlays.
        """
        import asyncio
        await asyncio.sleep(2)

        # Phase 1: JS click by ID (bypasses overlays)
        try:
            clicked = await page.evaluate('''
                (() => {
                    const btn = document.querySelector("#onetrust-accept-btn-handler");
                    if (btn) { btn.click(); return true; }
                    return false;
                })()
            ''')
            if clicked:
                await asyncio.sleep(1)
                logger.info("Cookie consent accepted (JS click)")
                return
        except Exception:
            pass

        # Phase 2: Playwright force-click by ID
        try:
            btn = await page.query_selector("#onetrust-accept-btn-handler")
            if btn:
                await btn.click(force=True)
                await asyncio.sleep(1)
                logger.info("Cookie consent accepted (force click)")
                return
        except Exception:
            pass

        logger.debug("Cookie consent button not found (may not be present)")

    async def _extract_listings(self, page: Any) -> list[RawListing]:
        """Extract all listing cards from current page (REF/MyHome.ie/2.md)."""
        listings = []

        # MyHome listing cards: app-standard-results-card component
        cards = await page.query_selector_all("app-standard-results-card")
        if not cards:
            cards = await page.query_selector_all("div.card.standard-card")
        if not cards:
            cards = await page.query_selector_all("div.card.property-card")

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
        """Parse a single property card (REF/MyHome.ie/2.md structure).
        
        Structure:
            h2.card-title  →  price (e.g. "€2,600 / month")
            h3.card-text   →  address
            span[aria-label="Beds"]  →  beds count
            span[aria-label="Bath"]  →  baths count
            standalone <span> after info strip  →  property type
            img.property-card__image  →  image
            a[href*="/rentals/brochure/"]  →  URL
        """
        # Get link: a[href*="/rentals/brochure/"]
        link = await card.query_selector('a[href*="/rentals/brochure/"]')
        if not link:
            link = await card.query_selector('a[href*="/rentals/"]')
        if not link:
            return None

        href = await link.get_attribute("href")
        if not href:
            return None

        url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
        source_id = self._extract_id_from_url(url)
        if not source_id:
            return None

        # Price: h2.card-title (e.g. "€2,600 / month")
        price_el = await card.query_selector("h2.card-title")
        price_text = await price_el.inner_text() if price_el else None

        # Address: h3.card-text (e.g. "Apartment 209, The New Hardwicke, Smithfield, Dublin 7")
        address_el = await card.query_selector("h3.card-text")
        location_text = await address_el.inner_text() if address_el else None

        # Title = address
        title = location_text

        # Beds: span[aria-label="Beds"] text
        beds_text = None
        beds_el = await card.query_selector('span[aria-label="Beds"]')
        if beds_el:
            text = await beds_el.inner_text()
            match = re.search(r"(\d+)", text)
            beds_text = match.group(1) if match else None

        # Baths: span[aria-label="Bath"] text
        baths_text = None
        baths_el = await card.query_selector('span[aria-label="Bath"]')
        if baths_el:
            text = await baths_el.inner_text()
            match = re.search(r"(\d+)", text)
            baths_text = match.group(1) if match else None

        # Image: img.property-card__image
        img_el = await card.query_selector("img.property-card__image")
        if not img_el:
            img_el = await card.query_selector("img[alt='Property Image']")
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
        """Extract listing ID from URL.
        
        MyHome: /rentals/brochure/description/12345678
        """
        match = re.search(r"/(\d+)(?:\?|$|/)", url)
        return match.group(1) if match else None

    async def _has_next_page(self, page: Any) -> bool:
        """Check if there's a next page (REF/MyHome.ie/3.md).
        
        Pagination: ul.ngx-pagination
        Next button: li.pagination-next — disabled when class includes "disabled"
        """
        next_btn = await page.query_selector("li.pagination-next:not(.disabled)")
        return next_btn is not None

    async def _go_to_next_page(self, page: Any) -> None:
        """Navigate to next page by clicking the Next button."""
        next_btn = await page.query_selector("li.pagination-next a")
        if next_btn:
            await next_btn.click()
            await page.wait_for_load_state("domcontentloaded")

    async def _enhance_with_jsonld(self, listing: RawListing, card: Any) -> RawListing:
        """
        Enhance listing with JSON-LD data as fallback for missing fields.
        """
        if listing.title and listing.price_text:
            return listing

        try:
            card_html = await card.inner_html()
            jsonld_data = extract_listing_from_html(card_html)

            if not jsonld_data:
                return listing

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
