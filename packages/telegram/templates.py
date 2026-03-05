"""
AGPARS Telegram Templates Module

Message templates and rendering for Telegram notifications.
"""


from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DEFAULT TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_LISTING_TEMPLATE = """🏠 *New Listing*

💰 *Price*: €{price}/month
🛏️ *Beds*: {beds}
📍 *Location*: {city}, {county}
🔗 [View Listing]({url})

_Source: {source}_"""

DEFAULT_DIGEST_TEMPLATE = """📊 *Daily Digest*

Found *{count}* new listings matching your criteria:

{listings}

_Last updated: {timestamp}_"""

DEFAULT_LISTING_IN_DIGEST = "• €{price} | {beds}BR | {city} | [Link]({url})"


# ═══════════════════════════════════════════════════════════════════════════════
# TEMPLATE RENDERING
# ═══════════════════════════════════════════════════════════════════════════════


def render_listing_message(
    listing: dict,
    template: str | None = None,
) -> str:
    """
    Render a single listing notification message.

    Args:
        listing: Listing data dict
        template: Custom template (uses default if None)

    Returns:
        Rendered message string (Markdown)
    """
    tpl = template or DEFAULT_LISTING_TEMPLATE

    # Prepare values with defaults
    values = {
        "price": _format_price(listing.get("price")),
        "beds": listing.get("beds", "?"),
        "baths": listing.get("baths", "?"),
        "city": listing.get("city", "Unknown"),
        "county": listing.get("county", "Unknown"),
        "area_text": listing.get("area_text", ""),
        "property_type": listing.get("property_type", "property"),
        "url": listing.get("url", ""),
        "source": listing.get("source", "unknown"),
        "first_photo_url": listing.get("first_photo_url", ""),
    }

    try:
        return tpl.format(**values)
    except KeyError as e:
        logger.warning("Template rendering error", missing_key=str(e))
        return DEFAULT_LISTING_TEMPLATE.format(**values)


def render_digest_message(
    listings: list[dict],
    template: str | None = None,
    item_template: str | None = None,
) -> str:
    """
    Render a digest message with multiple listings.

    Args:
        listings: List of listing data dicts
        template: Custom digest template
        item_template: Custom template for each listing line

    Returns:
        Rendered digest message (Markdown)
    """
    from datetime import datetime

    tpl = template or DEFAULT_DIGEST_TEMPLATE
    item_tpl = item_template or DEFAULT_LISTING_IN_DIGEST

    # Render individual listings
    listing_lines = []
    for listing in listings[:20]:  # Limit to 20 in digest
        line = item_tpl.format(
            price=_format_price(listing.get("price")),
            beds=listing.get("beds", "?"),
            city=listing.get("city", "Unknown"),
            url=listing.get("url", ""),
        )
        listing_lines.append(line)

    values = {
        "count": len(listings),
        "listings": "\n".join(listing_lines),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    }

    return tpl.format(**values)


def render_update_message(
    listing: dict,
    changes: dict,
) -> str:
    """
    Render an update notification for a changed listing.

    Args:
        listing: Current listing data
        changes: Dict of changed fields {field: (old, new)}

    Returns:
        Rendered update message (Markdown)
    """
    change_lines = []
    for field, (old_val, new_val) in changes.items():
        if field == "price":
            old_val = _format_price(old_val)
            new_val = _format_price(new_val)
        change_lines.append(f"• {field}: {old_val} → {new_val}")

    return f"""🔄 *Listing Updated*

💰 *Price*: €{_format_price(listing.get("price"))}/month
📍 *Location*: {listing.get("city", "Unknown")}, {listing.get("county", "Unknown")}

*Changes*:
{chr(10).join(change_lines)}

🔗 [View Listing]({listing.get("url", "")})"""


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def _format_price(price: int | float | str | None) -> str:
    """Format price with comma separators."""
    if price is None:
        return "N/A"
    try:
        return f"{int(float(price)):,}"
    except (ValueError, TypeError):
        return str(price)


def escape_markdown(text: str) -> str:
    """Escape special Markdown characters for Telegram."""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f"\\{char}")
    return text


def truncate_text(text: str, max_length: int = 200) -> str:
    """Truncate text to max length with ellipsis."""
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def validate_template(template: str) -> tuple[bool, list[str]]:
    """
    Validate a custom template for required placeholders.

    Returns:
        Tuple of (is_valid, list of missing placeholders)
    """
    required = ["{price}", "{city}", "{url}"]
    missing = [p for p in required if p not in template]
    return len(missing) == 0, missing
