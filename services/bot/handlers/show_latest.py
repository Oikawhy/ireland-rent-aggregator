"""
AGPARS "Show Latest" Handler

/latest command — displays most recent listings from pub.public_listings.

Covers T065.
"""

from packages.observability.logger import get_logger
from packages.telegram.templates import truncate_text
from services.bot.middleware.auth import AuthContext, require_workspace

logger = get_logger(__name__)

DEFAULT_LIMIT = 5


@require_workspace
async def handle_latest(update, context, auth_ctx: AuthContext) -> None:
    """
    /latest — Show the most recent listings matching workspace filters.
    """
    from packages.storage.subscriptions import get_subscriptions_for_workspace
    from services.bot.queries.listings import get_latest_listings

    # Get workspace filter context
    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id, enabled_only=True)
    filters = subs[0]["filters"] if subs else {}

    # Parse optional count arg: /latest 10
    args = (update.message.text or "").split()
    limit = DEFAULT_LIMIT
    if len(args) > 1:
        try:
            limit = min(int(args[1]), 20)  # Cap at 20
        except ValueError:
            pass

    # Query listings
    listings = get_latest_listings(filters=filters, limit=limit)

    if not listings:
        await update.message.reply_text(
            "📭 No listings found matching your filters.\n"
            "Try adjusting with /filter or wait for new listings.",
        )
        return

    # Format message
    lines = [f"📊 *Latest {len(listings)} Listings*\n"]
    for i, listing in enumerate(listings, 1):
        price = f"€{int(listing['price']):,}" if listing.get("price") else "N/A"
        beds = listing.get("beds") or "?"
        city = listing.get("city", "Unknown")
        county = listing.get("county", "")
        url = listing.get("url", "")
        source = listing.get("source", "")

        location = f"{city}, {county}" if county else city

        lines.append(
            f"{i}. {price} | {beds}BR | {location}\n"
            f"   [{source}]({url})"
        )

    message = "\n".join(lines)
    await update.message.reply_text(message, parse_mode="Markdown", disable_web_page_preview=True)

    logger.debug("Latest listings shown", workspace_id=auth_ctx.workspace_id, count=len(listings))
