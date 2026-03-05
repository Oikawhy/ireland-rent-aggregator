"""
AGPARS Callback Query Router

Central handler for all inline keyboard button presses.
Callback data format: action:param1:param2
"""

import asyncio

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import RetryAfter
from telegram.ext import ContextTypes

from packages.observability.logger import get_logger
from services.bot.message_manager import edit_menu_message, send_menu_message
from services.bot.handlers._callback_helpers import (
    PAGE_SIZE, safe_price, get_auth, get_sub, build_detail_text,
)

logger = get_logger(__name__)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    # Safely ack the callback — may fail if query is stale (flood clicking)
    try:
        await query.answer()
    except Exception:
        pass  # Stale query, still process the action

    data = query.data or ""
    parts = data.split(":")
    action = parts[0] if parts else ""

    try:
        handlers = {
            "menu": lambda: _show_main_menu(query, context),
            "browse": lambda: _handle_browse(query, context, parts),
            "detail": lambda: _handle_detail(query, context, parts),
            "hide": lambda: _handle_hide(query, context, parts),
            "hidden": lambda: _handle_hidden_list(query, context, parts),
            "hidden_detail": lambda: _handle_hidden_detail(query, context, parts),
            "unhide": lambda: _handle_unhide(query, context, parts),
            "fav_toggle": lambda: _handle_fav_toggle(query, context, parts),
            "favorites": lambda: _handle_favorites(query, context, parts),
            "fav_detail": lambda: _handle_fav_detail(query, context, parts),
            "digest_menu": lambda: _handle_digest_menu(query, context),
            "digest_on": lambda: _handle_digest_on(query, context),
            "digest_off": lambda: _handle_digest_off(query, context),
            "digest_daily": lambda: _handle_digest_daily_prompt(query, context),
            "digest_daily_range": lambda: _handle_digest_daily_range(query, context, parts),
            "digest_set_time": lambda: _handle_digest_set_time(query, context, parts),
            "digest_weekly": lambda: _handle_digest_weekly_days(query, context),
            "digest_weekly_day": lambda: _handle_digest_weekly_day(query, context, parts),
            "digest_weekly_range": lambda: _handle_digest_weekly_range(query, context, parts),
            "digest_weekly_time": lambda: _handle_digest_weekly_time(query, context, parts),
            "digest_pause": lambda: _handle_digest_pause(query, context),
            "digest_resume": lambda: _handle_digest_resume(query, context),
            "digest_page": lambda: _handle_digest_page(query, context, parts),
            "settings": lambda: _handle_settings(query, context),
            "set_pause": lambda: _handle_pause(query, context),
            "set_resume": lambda: _handle_resume(query, context),
            "stats": lambda: _handle_stats(query, context),
            "latest": lambda: _handle_latest(query, context),
            "filters": lambda: _handle_filters(query, context),
            "filter_price": lambda: _handle_filter_price_prompt(query, context),
            "filter_price_range": lambda: _handle_filter_price_range(query, context, parts),
            "filter_price_custom": lambda: _handle_filter_price_custom(query, context),
            "filter_price_from": lambda: _handle_filter_price_from(query, context, parts),
            "filter_price_fromto": lambda: _handle_filter_price_fromto(query, context, parts),
            "filter_price_set": lambda: _handle_filter_price_set(query, context, parts),
            "filter_price_clear": lambda: _handle_filter_price_clear(query, context),
            "filter_beds": lambda: _handle_filter_beds_prompt(query, context),
            "filter_beds_set": lambda: _handle_filter_beds_set(query, context, parts),
            "filter_beds_clear": lambda: _handle_filter_beds_clear(query, context),
            "filter_county": lambda: _handle_filter_county(query, context, parts),
            "county_toggle": lambda: _handle_county_toggle(query, context, parts),
            "county_all": lambda: _handle_county_all(query, context, parts),
            "city_list": lambda: _handle_city_list(query, context, parts),
            "city_toggle": lambda: _handle_city_toggle(query, context, parts),
            "city_clear": lambda: _handle_city_clear(query, context, parts),
            # Access control
            "request_access": lambda: _handle_request_access(query, context),
            "admin_accept": lambda: _handle_admin_accept(query, context, parts),
            "admin_decline": lambda: _handle_admin_decline(query, context, parts),
            # User management (admin)
            "users_menu": lambda: _handle_users_menu(query, context, parts),
            "users_page": lambda: _handle_users_menu(query, context, parts),
            "user_detail": lambda: _handle_user_detail(query, context, parts),
            "user_role": lambda: _handle_user_role(query, context, parts),
            "user_set_role": lambda: _handle_user_set_role(query, context, parts),
            "user_delete": lambda: _handle_user_delete(query, context, parts),
            "user_delete_confirm": lambda: _handle_user_delete_confirm(query, context, parts),
            "noop": lambda: None,
        }
        handler = handlers.get(action)
        if handler:
            result = handler()
            if result is not None:
                await result
        else:
            logger.warning("Unknown callback action", action=action)
    except RetryAfter as e:
        # Flood control — wait and retry the action once
        wait = min(e.retry_after + 1, 60)
        logger.warning("Flood control, retrying", action=action, wait=wait)
        await asyncio.sleep(wait)
        try:
            handler = handlers.get(action)
            if handler:
                result = handler()
                if result is not None:
                    await result
        except Exception as e2:
            logger.error("Retry after flood failed", action=action, error=str(e2))
            await _send_error_recovery(context, query.message.chat_id)
    except Exception as e:
        logger.error("Callback error", action=action, error=str(e))
        await _send_error_recovery(context, query.message.chat_id)


async def _send_error_recovery(context, chat_id: int) -> None:
    """Send a new menu message so the bot never disappears."""
    try:
        await send_menu_message(
            context, chat_id,
            "⚠️ Something went wrong\\. Here's the menu:",
            reply_markup=build_main_menu_keyboard(),
            parse_mode="Markdown",
        )
    except Exception:
        pass


def build_main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📋 Browse", callback_data="browse:1"),
         InlineKeyboardButton("📊 Latest", callback_data="latest")],
        [InlineKeyboardButton("⭐ Favorites", callback_data="favorites:1"),
         InlineKeyboardButton("🚫 Hidden", callback_data="hidden:1")],
        [InlineKeyboardButton("📬 Digest", callback_data="digest_menu"),
         InlineKeyboardButton("🔍 Filters", callback_data="filters")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
    ]
    if is_admin:
        rows.insert(-1, [InlineKeyboardButton("👥 Users", callback_data="users_menu"),
                         InlineKeyboardButton("📈 Stats", callback_data="stats")])
    return InlineKeyboardMarkup(rows)


async def _show_main_menu(query, context) -> None:
    from packages.storage.users import get_user_by_tg_id
    from packages.storage.models import UserRole
    bot_user = get_user_by_tg_id(query.from_user.id)
    is_admin = bot_user and bot_user["role"] == UserRole.ADMIN.value
    await edit_menu_message(context, query.message.chat_id, query.message.message_id,
                            "🏠 *AGPARS Rental Aggregator*\n\nSelect an option:",
                            reply_markup=build_main_menu_keyboard(is_admin=is_admin))


# ── Browse (buttons only) ────────────────────────────────────────────────

async def _handle_browse(query, context, parts) -> None:
    page = int(parts[1]) if len(parts) > 1 else 1
    chat_id = query.message.chat_id
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        await edit_menu_message(context, chat_id, query.message.message_id, "⚠️ Use /start first.")
        return
    from services.bot.queries.listings import get_latest_listings, get_listing_count
    from services.bot.queries.hidden import get_hidden_listing_ids
    sub = get_sub(auth_ctx)
    filters = sub["filters"] if sub else {}
    hidden_ids = get_hidden_listing_ids(auth_ctx.workspace_id)
    total = get_listing_count(filters=filters, exclude_ids=hidden_ids)
    offset = (page - 1) * PAGE_SIZE
    listings = get_latest_listings(filters=filters, limit=PAGE_SIZE, offset=offset, exclude_ids=hidden_ids)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if not listings:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu:main")]])
        await edit_menu_message(context, chat_id, query.message.message_id, "📭 No listings.", reply_markup=kb)
        return
    buttons = []
    for i, l in enumerate(listings, start=offset + 1):
        p, city = safe_price(l.get("price")), l.get("city", "?")
        buttons.append([InlineKeyboardButton(
            f"🔍 {i}. {p} — {l.get('beds') or '?'}BR — {city}",
            callback_data=f"detail:{l['listing_id']}:browse")])
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("◀️", callback_data=f"browse:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages: nav.append(InlineKeyboardButton("▶️", callback_data=f"browse:{page+1}"))
    buttons.append(nav)
    buttons.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])
    await edit_menu_message(context, chat_id, query.message.message_id,
                            f"📋 *Listings* — {page}/{total_pages} ({total} total)",
                            reply_markup=InlineKeyboardMarkup(buttons))


# ── Detail views (parse origin for correct Back) ─────────────────────────

async def _handle_detail(query, context, parts) -> None:
    listing_id = int(parts[1]) if len(parts) > 1 else 0
    origin = parts[2] if len(parts) > 2 else "browse"
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from services.bot.handlers._callbacks_part2 import _render_detail_inline, _back_for
    await _render_detail_inline(query, context, listing_id, auth_ctx,
                                 back_to=_back_for(origin), origin=origin)


async def _handle_hidden_detail(query, context, parts) -> None:
    listing_id = int(parts[1]) if len(parts) > 1 else 0
    from services.bot.queries.listings import get_listing_by_id
    listing = get_listing_by_id(listing_id)
    if not listing:
        await edit_menu_message(context, query.message.chat_id, query.message.message_id, "❌ Not found.")
        return
    rows = [[InlineKeyboardButton("♻️ Restore (unhide)",
                                   callback_data=f"unhide:{listing_id}:hidden")]]
    url = listing.get("url", "")
    if url:
        rows.append([InlineKeyboardButton("🔗 Open listing", url=url)])
    rows.append([InlineKeyboardButton("🔙 Hidden list", callback_data="hidden:1")])
    rows.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])
    await edit_menu_message(context, query.message.chat_id, query.message.message_id,
                            build_detail_text(listing), reply_markup=InlineKeyboardMarkup(rows))


async def _handle_fav_detail(query, context, parts) -> None:
    listing_id = int(parts[1]) if len(parts) > 1 else 0
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from services.bot.handlers._callbacks_part2 import _render_detail_inline, _back_for
    await _render_detail_inline(query, context, listing_id, auth_ctx,
                                 back_to=_back_for("fav"), origin="fav")


# ── Remaining handlers from split modules ────────────────────────────────

from services.bot.handlers._callbacks_part2 import (  # noqa: E402
    _handle_hide, _handle_unhide, _handle_hidden_list,
    _handle_fav_toggle, _handle_favorites,
    _handle_latest,
)

from services.bot.handlers._callbacks_part3 import (  # noqa: E402
    _handle_digest_menu, _handle_digest_on, _handle_digest_off,
    _handle_digest_daily_prompt, _handle_digest_daily_range,
    _handle_digest_set_time,
    _handle_digest_weekly_days, _handle_digest_weekly_day,
    _handle_digest_weekly_range, _handle_digest_weekly_time,
    _handle_digest_pause, _handle_digest_resume, _handle_digest_page,
)

from services.bot.handlers._callbacks_part4 import (  # noqa: E402
    _handle_settings, _handle_pause, _handle_resume,
    _handle_stats,
    _handle_filters, _handle_filter_price_prompt,
    _handle_filter_price_range,
    _handle_filter_price_custom, _handle_filter_price_from,
    _handle_filter_price_fromto,
    _handle_filter_price_set, _handle_filter_price_clear,
    _handle_filter_beds_prompt, _handle_filter_beds_set,
    _handle_filter_beds_clear, _handle_filter_county,
    _handle_county_toggle, _handle_county_all,
    _handle_city_list, _handle_city_toggle, _handle_city_clear,
)

from services.bot.handlers._callbacks_access import (  # noqa: E402
    handle_request_access as _handle_request_access,
    handle_admin_accept as _handle_admin_accept,
    handle_admin_decline as _handle_admin_decline,
)

from services.bot.handlers._callbacks_users import (  # noqa: E402
    handle_users_menu as _handle_users_menu,
    handle_user_detail as _handle_user_detail,
    handle_user_role as _handle_user_role,
    handle_user_set_role as _handle_user_set_role,
    handle_user_delete as _handle_user_delete,
    handle_user_delete_confirm as _handle_user_delete_confirm,
)
