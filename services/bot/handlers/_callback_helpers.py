"""
Shared helpers for callback handlers.
"""

import re

from packages.observability.logger import get_logger

logger = get_logger(__name__)

PAGE_SIZE = 10


DAYS_OF_WEEK = [
    ("Monday", "monday"), ("Tuesday", "tuesday"), ("Wednesday", "wednesday"),
    ("Thursday", "thursday"), ("Friday", "friday"),
    ("Saturday", "saturday"), ("Sunday", "sunday"),
]


def safe_price(price) -> str:
    if not price:
        return "N/A"
    try:
        return f"€{int(float(price)):,}"
    except (ValueError, TypeError):
        return f"€{price}"


def get_auth(query):
    from services.bot.middleware.auth import get_auth_context
    return get_auth_context(query.from_user.id, query.message.chat_id)


def get_sub(auth_ctx):
    from packages.storage.subscriptions import get_subscriptions_for_workspace
    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id, enabled_only=True)
    if not subs:
        subs = get_subscriptions_for_workspace(auth_ctx.workspace_id, enabled_only=False)
    return subs[0] if subs else None


def validate_time(time_str: str) -> bool:
    if not time_str or not isinstance(time_str, str):
        return False
    time_str = time_str.strip().replace(";", ":").replace(".", ":").replace(",", ":")
    match = re.match(r'^(\d{1,2}):(\d{2})$', time_str)
    if not match:
        return False
    hour, minute = int(match.group(1)), int(match.group(2))
    return 0 <= hour <= 23 and 0 <= minute <= 59


def build_detail_text(listing: dict) -> str:
    price = safe_price(listing.get("price"))
    beds = listing.get("beds")
    beds_str = str(beds) if beds is not None else "?"
    city = listing.get("city", "Unknown")
    county = listing.get("county", "")
    prop_type = listing.get("property_type", "property")
    source = listing.get("source", "")
    location = f"{city}, {county}" if county else city
    return (
        f"🏠 *Listing Detail*\n\n"
        f"💰 *Price*: {price}/month\n"
        f"🛏 *Beds*: {beds_str}\n"
        f"🏗 *Type*: {prop_type}\n"
        f"📍 *Location*: {location}\n"
        f"🌐 *Source*: {source}"
    )
