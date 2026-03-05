"""
AGPARS Rent.ie Adapter

County-based URL navigation scraping for rent.ie
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
# RENT.IE ADAPTER
# ═══════════════════════════════════════════════════════════════════════════════


# All 26 Republic of Ireland counties for iteration
COUNTIES = [
    "carlow", "cavan", "clare", "cork", "donegal", "dublin",
    "galway", "kerry", "kildare", "kilkenny", "laois", "leitrim",
    "limerick", "longford", "louth", "mayo", "meath", "monaghan",
    "offaly", "roscommon", "sligo", "tipperary", "waterford",
    "westmeath", "wexford", "wicklow",
]


class RentAdapter(BaseAdapter):
    """
    Adapter for Rent.ie

    Strategy: Direct URL navigation per county/city with pagination.
    URL patterns (from REF/Rent.ie/1.md):
      - County: https://www.rent.ie/houses-to-let/renting_county/
      - City:   https://www.rent.ie/houses-to-let/county/city-slug/
      - Area:   https://www.rent.ie/houses-to-let/renting_county/area-slug/
    """

    BASE_URL = "https://www.rent.ie"

    def get_source_name(self) -> str:
        return "rent"

    def get_search_url(self, city: str | None = None, county: str | None = None) -> str:
        """Build Rent.ie search URL using direct county/city URL pattern.
        
        REF/Rent.ie/1.md patterns:
          - County: https://www.rent.ie/houses-to-let/renting_county/
          - Area:   https://www.rent.ie/houses-to-let/renting_county/area-slug/
          - City:   https://www.rent.ie/houses-to-let/county/city-slug/
        
        For Dublin (city), the URL is: /houses-to-let/renting_dublin/
        """
        if city and county:
            # City within a county — use county/city pattern
            county_slug = county.lower().replace(" ", "-")
            city_slug = city.lower().replace(" ", "-").replace(".", "")
            return f"{self.BASE_URL}/houses-to-let/{county_slug}/{city_slug}/"
        elif city:
            # City without county — treat city as county (e.g. Dublin → renting_dublin)
            city_slug = city.lower().replace(" ", "-").replace(".", "")
            return f"{self.BASE_URL}/houses-to-let/renting_{city_slug}/"
        elif county:
            county_slug = county.lower().replace(" ", "-")
            return f"{self.BASE_URL}/houses-to-let/renting_{county_slug}/"
        else:
            # Fallback: dublin (should not be reached — scrape() iterates counties)
            return f"{self.BASE_URL}/houses-to-let/renting_dublin/"

    async def scrape(
        self,
        page: Any,
        city: str | None = None,
        county: str | None = None,
        max_pages: int | None = None,
    ) -> list[RawListing]:
        """Scrape listings from Rent.ie.

        When no city/county specified, iterates through all 26 counties
        using the pattern /houses-to-let/renting_{county}/ per REF/Rent.ie/1.md.
        """
        # If a specific county is given, scrape just that one
        if city or county:
            return await self._scrape_single(page, city, county, max_pages)

        # Otherwise iterate all counties
        all_listings = []
        cookie_handled = False

        for county_name in COUNTIES:
            try:
                county_listings = await self._scrape_single(
                    page, city=None, county=county_name,
                    max_pages=max_pages, skip_cookie=(cookie_handled),
                )
                all_listings.extend(county_listings)
                cookie_handled = True  # Only handle cookie on first county

                self.logger.info(
                    "County scraped",
                    county=county_name,
                    found=len(county_listings),
                    total_so_far=len(all_listings),
                )
            except Exception as e:
                self.logger.warning(
                    "County scrape failed, continuing",
                    county=county_name,
                    error=str(e),
                )
                continue

        self.logger.info("Full scrape completed", counties=len(COUNTIES), total=len(all_listings))
        return all_listings

    async def _scrape_single(
        self,
        page: Any,
        city: str | None = None,
        county: str | None = None,
        max_pages: int | None = None,
        skip_cookie: bool = False,
    ) -> list[RawListing]:
        """Scrape listings for a single county/city.

        Uses URL-based pagination: /page_2/, /page_3/, etc.
        Falls back to clicking Next if URL construction fails.
        """
        base_url = self.get_search_url(city, county)
        listings = []

        try:
            # Page 1
            await page.goto(base_url, wait_until="domcontentloaded")
            await page_load_delay()
            await human_scroll(page)
            await human_mouse_jitter(page)

            if not skip_cookie:
                await self._handle_cookie_consent(page)

            # Extract page 1
            page_listings = await self._extract_listings(page)
            listings.extend(page_listings)
            self.logger.info("Page scraped", page=1, county=county, found=len(page_listings))

            if not page_listings:
                return listings

            # Check if pagination exists on page 1
            has_pagination = await self._has_next_page(page)
            if not has_pagination:
                self.logger.debug("No pagination found, single page", county=county)
                return listings

            # Pages 2+: URL-based pagination
            page_limit = max_pages or 200
            for page_num in range(2, page_limit + 1):
                page_url = self._build_page_url(base_url, page_num)
                await between_pages_delay()

                await page.goto(page_url, wait_until="domcontentloaded")
                await page_load_delay()
                await human_scroll(page)

                page_listings = await self._extract_listings(page)
                listings.extend(page_listings)
                self.logger.info(
                    "Page scraped", page=page_num, county=county,
                    found=len(page_listings), total=len(listings),
                )

                # Stop if no listings (past last page)
                if not page_listings:
                    self.logger.info("No more listings", county=county, last_page=page_num)
                    break

        except Exception as e:
            self.logger.error("Scrape failed", error=str(e), city=city, county=county)
            raise

        self.logger.info("Scrape completed", city=city, county=county, total=len(listings))
        return listings

    async def _handle_cookie_consent(self, page: Any) -> None:
        """Handle cookie consent popup."""
        try:
            consent_btn = await page.query_selector("#onetrust-accept-btn-handler")
            if consent_btn:
                await consent_btn.click()
                await page.wait_for_timeout(500)
        except Exception:
            pass

    async def _extract_listings(self, page: Any) -> list[RawListing]:
        """Extract all listing cards from current page."""
        listings = []

        # REF/Rent.ie/2.md: listings are in div.search_result
        cards = await page.query_selector_all("div.search_result")
        
        # Debug: log what the page looks like
        if not cards:
            try:
                title = await page.evaluate("document.title") if hasattr(page, 'evaluate') else "N/A"
                url = await page.evaluate("window.location.href") if hasattr(page, 'evaluate') else "N/A"
                self.logger.warning("No cards found on page",
                                    page_title=str(title)[:100],
                                    page_url=str(url)[:200])
                # Try alternative selectors
                alt_cards = await page.query_selector_all(".search_result, .sresult_container, [class*='result']")
                self.logger.debug("Alternative selector count", count=len(alt_cards))
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
        """Parse a single property card (REF/Rent.ie/2.md structure)."""
        # Get link from address header: .sresult_address h2 a
        link = await card.query_selector(".sresult_address h2 a")
        if not link:
            link = await card.query_selector("a[href*='/houses-to-let/']")
        if not link:
            return None

        href = await link.get_attribute("href")
        if not href:
            return None

        url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
        source_id = self._extract_id_from_url(url)
        if not source_id:
            return None

        # Title = address text from the link
        title = await link.inner_text()

        # Price: .sresult_description h4 (e.g. "€1,550 monthly")
        price_el = await card.query_selector(".sresult_description h4")
        price_text = await price_el.inner_text() if price_el else None

        # Details: .sresult_description h3 (e.g. "3 bedrooms (3 double), 2 bathrooms, furnished")
        details_el = await card.query_selector(".sresult_description h3")
        details_text = await details_el.inner_text() if details_el else None

        beds_text = None
        baths_text = None
        if details_text:
            beds_match = re.search(r"(\d+)\s*bedroom", details_text, re.I)
            baths_match = re.search(r"(\d+)\s*bathroom", details_text, re.I)
            beds_text = beds_match.group(1) if beds_match else None
            baths_text = baths_match.group(1) if baths_match else None

        # Location = title (address)
        location_text = title

        # Image: .sresult_image_container img.sresult_thumb
        img_el = await card.query_selector(".sresult_image_container img.sresult_thumb")
        if not img_el:
            img_el = await card.query_selector("img.sresult_thumb")
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
                "details_text": details_text,
            },
            title=sanitize_text(title),
            price_text=price_text,
            beds_text=beds_text,
            baths_text=baths_text,
            location_text=sanitize_text(location_text),
        )

    def _extract_id_from_url(self, url: str) -> str | None:
        """Extract listing ID from URL.
        
        Rent.ie URL pattern: /houses-to-let/Name-Address/ID/
        """
        match = re.search(r"/(\d{5,})/", url)
        if match:
            return match.group(1)
        # Fallback
        match = re.search(r"/(\d+)/?(?:\?|$)", url)
        return match.group(1) if match else None

    def _build_page_url(self, base_url: str, page_num: int) -> str:
        """Build paginated URL for Rent.ie.

        Pattern: {base_url}page_{N}/
        Example: /houses-to-let/renting_dublin/page_55/
        """
        if page_num <= 1:
            return base_url
        base = base_url.rstrip("/") + "/"
        return f"{base}page_{page_num}/"

    async def _has_next_page(self, page: Any) -> bool:
        """Check if there's a next page (REF/Rent.ie/4.md).

        Correct structure:
          <div id="pages">
            <span class="nextprev_on"><a href="/page_2/">Next »</a></span>
          </div>
        """
        # Primary: correct selector from REF
        next_link = await page.query_selector('div#pages span.nextprev_on a')
        if next_link:
            return True
        # Fallback: any link in #pages containing "Next"
        next_text = await page.query_selector('#pages a:has-text("Next")')
        return next_text is not None

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
