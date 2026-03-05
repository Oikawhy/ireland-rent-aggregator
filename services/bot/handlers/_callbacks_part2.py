"""
AGPARS Callback Handlers Part 2

Hide/Unhide (inline toggle), Hidden list, Favorites (inline toggle), Latest.
All detail views thread 'origin' so the Back button returns to the correct menu.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from packages.observability.logger import get_logger
from services.bot.message_manager import edit_menu_message
from services.bot.handlers._callback_helpers import (
    PAGE_SIZE, safe_price, get_auth, get_sub, build_detail_text,
)

logger = get_logger(__name__)


# ── Origin → Back mapping ────────────────────────────────────────────────

_ORIGIN_BACK = {
    "browse": "browse:1",
    "latest": "latest",
    "hidden": "hidden:1",
    "fav": "favorites:1",
}


def _back_for(origin: str) -> str:
    return _ORIGIN_BACK.get(origin, "browse:1")


# ═══════════════════════════════════════════════════════════════════════════════
# HIDE / UNHIDE (inline toggle — re-renders detail view)
# ═══════════════════════════════════════════════════════════════════════════════


async def _handle_hide(query, context, parts) -> None:
    listing_id = int(parts[1]) if len(parts) > 1 else 0
    origin = parts[2] if len(parts) > 2 else "browse"
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from services.bot.queries.hidden import hide_listing
    hide_listing(auth_ctx.workspace_id, listing_id, query.from_user.id)
    await _render_detail_inline(query, context, listing_id, auth_ctx,
                                 back_to=_back_for(origin), origin=origin)


async def _handle_unhide(query, context, parts) -> None:
    listing_id = int(parts[1]) if len(parts) > 1 else 0
    origin = parts[2] if len(parts) > 2 else "browse"
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from services.bot.queries.hidden import unhide_listing
    unhide_listing(auth_ctx.workspace_id, listing_id)
    await _render_detail_inline(query, context, listing_id, auth_ctx,
                                 back_to=_back_for(origin), origin=origin)


# ═══════════════════════════════════════════════════════════════════════════════
# FAVORITES (inline toggle — re-renders detail view)
# ═══════════════════════════════════════════════════════════════════════════════


async def _handle_fav_toggle(query, context, parts) -> None:
    listing_id = int(parts[1]) if len(parts) > 1 else 0
    origin = parts[2] if len(parts) > 2 else "browse"
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from services.bot.queries.favorites import toggle_favorite
    toggle_favorite(auth_ctx.workspace_id, listing_id, query.from_user.id)
    await _render_detail_inline(query, context, listing_id, auth_ctx,
                                 back_to=_back_for(origin), origin=origin)


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED: Re-render detail view inline
# ═══════════════════════════════════════════════════════════════════════════════


async def _render_detail_inline(query, context, listing_id: int, auth_ctx,
                                 back_to: str = "browse:1",
                                 origin: str = "browse") -> None:
    """Re-render detail view with current fav/hide state, threading origin."""
    from services.bot.queries.listings import get_listing_by_id
    listing = get_listing_by_id(listing_id)
    if not listing:
        await edit_menu_message(context, query.message.chat_id,
                                query.message.message_id, "❌ Not found.")
        return

    fav = "☆"
    if auth_ctx and auth_ctx.has_workspace:
        from services.bot.queries.favorites import is_favorite
        if is_favorite(auth_ctx.workspace_id, listing_id):
            fav = "⭐"

    from services.bot.queries.hidden import is_hidden
    hidden = is_hidden(auth_ctx.workspace_id, listing_id) if auth_ctx and auth_ctx.has_workspace else False

    # Thread origin through sub-action buttons
    row1 = [InlineKeyboardButton(f"{fav} Favorite",
                                  callback_data=f"fav_toggle:{listing_id}:{origin}")]
    if hidden:
        row1.append(InlineKeyboardButton("🔓 Unhide",
                                          callback_data=f"unhide:{listing_id}:{origin}"))
    else:
        row1.append(InlineKeyboardButton("🚫 Hide",
                                          callback_data=f"hide:{listing_id}:{origin}"))

    rows = [row1]
    url = listing.get("url", "")
    if url:
        rows.append([InlineKeyboardButton("🔗 Open listing", url=url)])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=back_to)])
    rows.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])

    await edit_menu_message(context, query.message.chat_id, query.message.message_id,
                            build_detail_text(listing),
                            reply_markup=InlineKeyboardMarkup(rows))


# ═══════════════════════════════════════════════════════════════════════════════
# HIDDEN LIST
# ═══════════════════════════════════════════════════════════════════════════════


async def _handle_hidden_list(query, context, parts) -> None:
    page = int(parts[1]) if len(parts) > 1 else 1
    chat_id = query.message.chat_id
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from services.bot.queries.hidden import get_hidden_listings, get_hidden_count
    offset = (page - 1) * PAGE_SIZE
    total = get_hidden_count(auth_ctx.workspace_id)
    hidden = get_hidden_listings(auth_ctx.workspace_id, limit=PAGE_SIZE, offset=offset)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    if not hidden:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu:main")]])
        await edit_menu_message(context, chat_id, query.message.message_id,
                                "🚫 No hidden listings.", reply_markup=kb)
        return

    buttons = []
    for i, l in enumerate(hidden, start=offset + 1):
        p, city, lid = safe_price(l.get("price")), l.get("city", "?"), l.get("listing_id")
        buttons.append([InlineKeyboardButton(f"🔍 {i}. {p} — {city}",
                                              callback_data=f"hidden_detail:{lid}")])
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("◀️", callback_data=f"hidden:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages: nav.append(InlineKeyboardButton("▶️", callback_data=f"hidden:{page+1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])

    await edit_menu_message(context, chat_id, query.message.message_id,
                            f"🚫 *Hidden Listings* — {page}/{total_pages} ({total} total)",
                            reply_markup=InlineKeyboardMarkup(buttons))


# ═══════════════════════════════════════════════════════════════════════════════
# FAVORITES LIST
# ═══════════════════════════════════════════════════════════════════════════════


async def _handle_favorites(query, context, parts) -> None:
    page = int(parts[1]) if len(parts) > 1 else 1
    chat_id = query.message.chat_id
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from services.bot.queries.favorites import get_favorites, get_favorites_count
    offset = (page - 1) * PAGE_SIZE
    total = get_favorites_count(auth_ctx.workspace_id)
    favs = get_favorites(auth_ctx.workspace_id, limit=PAGE_SIZE, offset=offset)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    if not favs:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu:main")]])
        await edit_menu_message(context, chat_id, query.message.message_id,
                                "⭐ No favorites yet.", reply_markup=kb)
        return

    buttons = []
    for i, l in enumerate(favs, start=offset + 1):
        p, city, lid = safe_price(l.get("price")), l.get("city", "?"), l.get("listing_id")
        buttons.append([InlineKeyboardButton(f"⭐ {i}. {p} — {city}",
                                              callback_data=f"fav_detail:{lid}")])
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("◀️", callback_data=f"favorites:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages: nav.append(InlineKeyboardButton("▶️", callback_data=f"favorites:{page+1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])
    await edit_menu_message(context, chat_id, query.message.message_id,
                            f"⭐ *Favorites* — {page}/{total_pages} ({total} total)",
                            reply_markup=InlineKeyboardMarkup(buttons))


# ═══════════════════════════════════════════════════════════════════════════════
# LATEST (button-style, threads origin="latest")
# ═══════════════════════════════════════════════════════════════════════════════


async def _handle_latest(query, context) -> None:
    chat_id = query.message.chat_id
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from services.bot.queries.listings import get_latest_listings
    from services.bot.queries.hidden import get_hidden_listing_ids
    sub = get_sub(auth_ctx)
    filters = sub["filters"] if sub else {}
    hidden_ids = get_hidden_listing_ids(auth_ctx.workspace_id)
    listings = get_latest_listings(filters=filters, limit=5, exclude_ids=hidden_ids)
    if not listings:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu:main")]])
        await edit_menu_message(context, chat_id, query.message.message_id,
                                "📭 No listings.", reply_markup=kb)
        return
    buttons = []
    for i, l in enumerate(listings, 1):
        p, city, lid = safe_price(l.get("price")), l.get("city", "?"), l.get("listing_id")
        buttons.append([InlineKeyboardButton(
            f"🔍 {i}. {p} — {l.get('beds') or '?'}BR — {city}",
            callback_data=f"detail:{lid}:latest")])
    buttons.append([InlineKeyboardButton("📋 Browse all", callback_data="browse:1")])
    buttons.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])
    await edit_menu_message(context, chat_id, query.message.message_id,
                            f"📊 *Latest {len(listings)} Listings*",
                            reply_markup=InlineKeyboardMarkup(buttons))
