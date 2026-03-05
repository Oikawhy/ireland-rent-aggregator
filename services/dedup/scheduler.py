"""
AGPARS Dedup Scheduler

Group-by-county+price dedup: instead of brute-force comparing 100 random
listings, groups all active listings by (county, price_band) and runs
cross-source matching within each group.

Covers T076.
"""

import asyncio
from collections import defaultdict

from packages.observability.logger import get_logger
from packages.observability.metrics import LISTING_LINKS_CREATED_TOTAL

logger = get_logger(__name__)

# Price band width — listings within ±5% are in the same band
PRICE_BAND_PCT = 0.05


def _price_band(price) -> int | None:
    """Round price to nearest band for grouping."""
    if not price or float(price) <= 0:
        return None
    p = float(price)
    # Band width = max(50, 5% of price)
    band = max(50, int(p * PRICE_BAND_PCT))
    return int(p // band) * band


async def run_dedup_cycle() -> dict:
    """
    Run one full dedup cycle for all active listings.

    Groups by county + price_band, compares only within groups.
    O(groups × group_size²) instead of O(N²).
    """
    from services.bot.queries.listings import get_latest_listings
    from services.dedup.linker import find_cross_source_matches
    from packages.storage.listing_links import create_listing_link

    stats = {
        "total_checked": 0,
        "groups_processed": 0,
        "cross_links_created": 0,
    }

    # Fetch ALL active listings (no limit — we group them efficiently)
    listings = get_latest_listings(limit=10000)
    stats["total_checked"] = len(listings)

    if not listings:
        logger.info("No listings to dedup")
        return stats

    # Group by county + price_band
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for listing in listings:
        county = (listing.get("county") or "unknown").lower()
        band = _price_band(listing.get("price"))
        groups[(county, band)].append(listing)

    # Process each group
    for group_key, group in groups.items():
        # Skip groups with only one source
        sources = {l.get("source") for l in group}
        if len(sources) < 2:
            continue

        stats["groups_processed"] += 1

        # Cross-source matching within the group
        for i, listing in enumerate(group):
            others = group[i + 1:]
            matches = find_cross_source_matches(listing, others)

            for match in matches:
                try:
                    raw_id_a = listing.get("raw_id")
                    raw_id_b = match["listing"].get("raw_id")

                    if raw_id_a and raw_id_b:
                        link_id = create_listing_link(
                            raw_id_a=raw_id_a,
                            raw_id_b=raw_id_b,
                            confidence=match["score"],
                            reason="cross_source",
                        )
                        if link_id:
                            stats["cross_links_created"] += 1
                except Exception as e:
                    logger.warning("Link creation failed", error=str(e))

    logger.info(
        "Dedup cycle complete",
        checked=stats["total_checked"],
        groups=stats["groups_processed"],
        links_created=stats["cross_links_created"],
    )

    if stats["cross_links_created"] > 0:
        LISTING_LINKS_CREATED_TOTAL.labels(confidence_bucket="high").inc(stats["cross_links_created"])

    return stats


async def run_dedup_loop(interval_seconds: int = 300):
    """Run dedup cycles on a schedule."""
    logger.info("Dedup scheduler started", interval=interval_seconds)

    while True:
        try:
            stats = await run_dedup_cycle()
            logger.info("Dedup cycle finished", **stats)
        except Exception as e:
            logger.error("Dedup cycle error", error=str(e))

        await asyncio.sleep(interval_seconds)
