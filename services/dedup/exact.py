"""
AGPARS Exact Deduplication

URL-based and fingerprint-based exact duplicate detection.

Covers T075.
"""

import hashlib
import json

from packages.observability.logger import get_logger

logger = get_logger(__name__)


def generate_fingerprint(listing: dict) -> str:
    """
    Generate SHA-256 fingerprint for a listing.

    Uses URL + source as primary key. Falls back to
    price + beds + address for URL-less listings.

    Returns:
        64-char hex fingerprint
    """
    url = listing.get("url", "")
    if url:
        key = url
    else:
        # Fallback: composite key
        key = json.dumps({
            "source": listing.get("source", ""),
            "price": listing.get("price"),
            "beds": listing.get("beds"),
            "area_text": listing.get("area_text", ""),
        }, sort_keys=True)

    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def is_exact_duplicate(listing_a: dict, listing_b: dict) -> bool:
    """
    Check if two listings are exact duplicates.

    Same URL = exact duplicate (regardless of source).

    Returns:
        True if exact duplicate
    """
    url_a = listing_a.get("url", "")
    url_b = listing_b.get("url", "")

    if url_a and url_b:
        return url_a == url_b

    return generate_fingerprint(listing_a) == generate_fingerprint(listing_b)


def deduplicate_batch(listings: list[dict]) -> list[dict]:
    """
    Remove exact duplicates from a batch of listings.

    Keeps the first occurrence of each unique listing.

    Returns:
        Deduplicated list
    """
    seen: set[str] = set()
    unique: list[dict] = []

    for listing in listings:
        fp = generate_fingerprint(listing)
        if fp not in seen:
            seen.add(fp)
            unique.append(listing)
        else:
            logger.debug("Exact duplicate removed", url=listing.get("url", ""))

    removed = len(listings) - len(unique)
    if removed:
        logger.info("Batch deduplication", total=len(listings), unique=len(unique), removed=removed)

    return unique
