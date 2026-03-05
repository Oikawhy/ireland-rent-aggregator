"""
AGPARS Browse Handler

/browse — Paginated listing display with inline keyboards.

Covers T065.10.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from packages.observability.logger import get_logger
from services.bot.message_manager import send_menu_message
from services.bot.middleware.auth import AuthContext, require_workspace
from services.bot.queries.filters import describe_filters
from services.bot.queries.hidden import get_hidden_listing_ids
from services.bot.queries.listings import get_latest_listings, get_listing_count

logger = get_logger(__name__)

PAGE_SIZE = 10


def _safe_price(price) -> str:
    """Format price safely."""
    if not price:
        return "N/A"
    try:
        return f"€{int(float(price)):,}"
    except (ValueError, TypeError):
        return f"€{price}"


@require_workspace
async def handle_browse(update, context, auth_ctx: AuthContext) -> None:
    """
    /browse — Browse listings with inline keyboard pagination.
    """
    from packages.storage.subscriptions import get_subscriptions_for_workspace

    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id, enabled_only=True)
    filters = subs[0]["filters"] if subs else {}

    hidden_ids = get_hidden_listing_ids(auth_ctx.workspace_id)

    total = get_listing_count(filters=filters, exclude_ids=hidden_ids)
    listings = get_latest_listings(
        filters=filters, limit=PAGE_SIZE, offset=0, exclude_ids=hidden_ids
    )
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    if not listings:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Menu", callback_data="menu:main")],
        ])
        await send_menu_message(
            context, update.effective_chat.id,
            "📭 No listings found.",
            reply_markup=keyboard,
        )
        return

    # Build listing text + buttons
    lines = [f"📋 *Listings* — page 1/{total_pages} ({total} total)\n"]
    buttons = []

    for i, listing in enumerate(listings, start=1):
        price = _safe_price(listing.get("price"))
        beds = listing.get("beds") or "?"
        city = listing.get("city", "Unknown")
        source = listing.get("source", "")
        listing_id = listing.get("listing_id")

        lines.append(f"{i}. {price} | {beds}BR | {city} | _{source}_")
        buttons.append([
            InlineKeyboardButton(
                f"🔍 {i}. {price} — {city}",
                callback_data=f"detail:{listing_id}",
            )
        ])

    # Pagination row
    nav_row = [InlineKeyboardButton("1/{}".format(total_pages), callback_data="noop")]
    if total_pages > 1:
        nav_row.append(InlineKeyboardButton("▶️", callback_data="browse:2"))
    buttons.append(nav_row)
    buttons.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])

    keyboard = InlineKeyboardMarkup(buttons)
    message = "\n".join(lines)

    await send_menu_message(
        context, update.effective_chat.id, message,
        reply_markup=keyboard,
    )
