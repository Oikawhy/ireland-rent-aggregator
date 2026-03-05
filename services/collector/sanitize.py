"""
AGPARS Scraper Input Sanitization

Sanitization of data extracted from rental websites.
"""

import html
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT SANITIZATION
# ═══════════════════════════════════════════════════════════════════════════════


def sanitize_text(text: str | None, max_length: int = 500) -> str | None:
    """
    Sanitize text extracted from web pages.

    - Decodes HTML entities
    - Removes excessive whitespace
    - Strips control characters
    - Limits length
    """
    if not text:
        return None

    # Decode HTML entities
    cleaned = html.unescape(text)

    # Remove control characters (except newlines/tabs)
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]", "", cleaned)

    # Normalize whitespace
    cleaned = re.sub(r"\s+", " ", cleaned)

    # Strip and limit length
    cleaned = cleaned.strip()[:max_length]

    return cleaned if cleaned else None


def sanitize_title(title: str | None) -> str | None:
    """Sanitize listing title."""
    if not title:
        return None

    cleaned = sanitize_text(title, max_length=255)
    if not cleaned:
        return None

    # Remove common noise phrases
    noise_patterns = [
        r"^(rent|for rent|to rent|available)[\s:-]+",
        r"[\s-]+(available now|available immediately)$",
    ]
    for pattern in noise_patterns:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)

    return cleaned.strip() if cleaned.strip() else None


# ═══════════════════════════════════════════════════════════════════════════════
# NUMERIC SANITIZATION
# ═══════════════════════════════════════════════════════════════════════════════


def sanitize_price(price_text: str | None) -> Decimal | None:
    """
    Extract and sanitize price from text.

    Examples:
        "€1,500 per month" -> Decimal("1500")
        "1500/mth" -> Decimal("1500")
        "€1.5k" -> Decimal("1500")
        "€300 per week" -> Decimal("1200")  (weekly × 4)
    """
    if not price_text:
        return None

    try:
        # Detect weekly pricing BEFORE stripping (need original text context)
        weekly = bool(re.search(
            r"(?:per\s*week|/\s*w(?:ee)?k|weekly|/\s*wk|\bpw\b)",
            price_text,
            re.IGNORECASE,
        ))

        # Remove currency symbols and whitespace
        cleaned = re.sub(r"[€$£\s,]", "", price_text)

        # Strip weekly/monthly period indicators so they don't interfere
        cleaned = re.sub(
            r"(?:per|/)?(?:week|weekly|wk|pw|month|mth|mo|pm|pcm).*$",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )

        # Handle 'k' notation (e.g., "1.5k") — only when a digit precedes "k"
        if re.search(r"\dk$", cleaned, re.IGNORECASE):
            cleaned = cleaned[:-1]
            multiplier = 1000
        else:
            multiplier = 1

        # Extract first numeric part
        match = re.search(r"(\d+(?:\.\d+)?)", cleaned)
        if match:
            price = Decimal(match.group(1)) * multiplier

            # Convert weekly to monthly
            if weekly:
                price = price * 4

            # Sanity check
            if price < 0 or price > 100000:
                logger.warning("Price out of range", raw=price_text, parsed=price)
                return None

            return price

    except (InvalidOperation, ValueError) as e:
        logger.debug("Failed to parse price", raw=price_text, error=str(e))

    return None


def sanitize_beds(beds_text: str | None) -> int | None:
    """
    Extract bedroom count from text.

    Examples:
        "3 bed" -> 3
        "2BR" -> 2
        "Studio" -> 0
    """
    if not beds_text:
        return None

    beds_lower = beds_text.lower().strip()

    # Studio detection
    if "studio" in beds_lower:
        return 0

    # Extract numeric
    match = re.search(r"(\d+)", beds_lower)
    if match:
        beds = int(match.group(1))
        if 0 <= beds <= 20:
            return beds

    return None


def sanitize_baths(baths_text: str | None) -> int | None:
    """Extract bathroom count from text."""
    if not baths_text:
        return None

    match = re.search(r"(\d+)", baths_text)
    if match:
        baths = int(match.group(1))
        if 0 <= baths <= 10:
            return baths

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# URL SANITIZATION
# ═══════════════════════════════════════════════════════════════════════════════


def sanitize_url(url: str | None, allowed_domains: list[str] | None = None) -> str | None:
    """
    Validate and sanitize a URL.

    Returns None if URL is invalid or not from allowed domains.
    """
    if not url:
        return None

    try:
        url = url.strip()

        # Ensure scheme
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"

        parsed = urlparse(url)

        # Validate structure
        if not parsed.netloc or not parsed.scheme:
            return None

        # Check allowed domains
        if allowed_domains:
            domain = parsed.netloc.lower()
            if not any(domain.endswith(d.lower()) for d in allowed_domains):
                logger.warning("URL domain not allowed", url=url)
                return None

        # Limit length
        if len(url) > 2048:
            return None

        return url

    except Exception as e:
        logger.debug("Failed to parse URL", url=url, error=str(e))
        return None


def extract_listing_id(url: str, source: str) -> str | None:
    """
    Extract source-specific listing ID from URL.

    Examples:
        daft.ie/123456 -> "123456"
        rent.ie/property/abc123 -> "abc123"
    """
    patterns = {
        "daft": r"/(\d+)(?:[/?#]|$)",
        "rent": r"/property/([a-zA-Z0-9-]+)",
        "property": r"/(\d+)(?:[/?#]|$)",
        "myhome": r"/(\d+)(?:[/?#]|$)",
        "dng": r"/property/([a-zA-Z0-9-]+)",
        "sherryfitz": r"/property/([a-zA-Z0-9-]+)",
    }

    pattern = patterns.get(source.lower().replace(".ie", ""))
    if pattern:
        match = re.search(pattern, url)
        if match:
            return match.group(1)

    # Fallback: use URL hash
    import hashlib
    return hashlib.md5(url.encode()).hexdigest()[:16]


# ═══════════════════════════════════════════════════════════════════════════════
# LOCATION SANITIZATION
# ═══════════════════════════════════════════════════════════════════════════════


def sanitize_location(location_text: str | None) -> dict[str, str | None]:
    """
    Parse and sanitize location text into components.

    Returns:
        Dict with keys: city, county, area_text
    """
    result = {"city": None, "county": None, "area_text": None}

    if not location_text:
        return result

    cleaned = sanitize_text(location_text, max_length=500)
    if not cleaned:
        return result

    result["area_text"] = cleaned

    # Try to extract county (Co. X, County X)
    county_match = re.search(r"(?:Co\.?|County)\s+([A-Za-z]+)", cleaned, re.IGNORECASE)
    if county_match:
        result["county"] = county_match.group(1).title()

    # Dublin special case
    if "dublin" in cleaned.lower():
        result["county"] = "Dublin"

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY TYPE DETECTION
# ═══════════════════════════════════════════════════════════════════════════════


def detect_property_type(text: str | None) -> str | None:
    """Detect property type from text."""
    if not text:
        return None

    text_lower = text.lower()

    if "studio" in text_lower:
        return "studio"
    if any(w in text_lower for w in ["apartment", "apt", "flat"]):
        return "apartment"
    if any(w in text_lower for w in ["house", "home", "cottage", "bungalow"]):
        return "house"

    return "other"


# Alias for normalize.py compatibility
def sanitize_property_type(text: str | None) -> str | None:
    """Sanitize and detect property type. Alias for detect_property_type."""
    from packages.storage.models import PropertyType

    result = detect_property_type(text)
    if result:
        try:
            return PropertyType(result)
        except ValueError:
            return PropertyType.OTHER
    return None
