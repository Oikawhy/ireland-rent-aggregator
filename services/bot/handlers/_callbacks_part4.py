"""
AGPARS Callback Handlers Part 4

Settings (inline pause/resume), Stats, Filters (2-level price/beds/county).
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from packages.observability.logger import get_logger
from packages.core.validation import VALID_COUNTIES
from services.bot.message_manager import edit_menu_message
from services.bot.handlers._callback_helpers import (
    PAGE_SIZE, safe_price, get_auth, get_sub,
)

logger = get_logger(__name__)

# Price steps for custom FROM/TO range picker
_PRICE_STEPS = list(range(500, 10001, 500))  # 500, 1000, 1500, ..., 10000


# ═══════════════════════════════════════════════════════════════════════════════
# SETTINGS (inline refresh after pause/resume)
# ═══════════════════════════════════════════════════════════════════════════════


async def _handle_settings(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from packages.storage.subscriptions import get_subscriptions_for_workspace
    ws = auth_ctx.workspace
    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id, enabled_only=False)
    text = (f"⚙️ *Workspace Settings*\n\n"
            f"Name: *{ws.get('title') or 'Untitled'}*\n"
            f"Timezone: {ws.get('timezone', 'UTC')}\n"
            f"Active: {'✅' if ws.get('is_active', True) else '❌'}\n\n")
    if subs:
        text += "*Subscriptions:*\n"
        for s in subs:
            paused = (not s["is_enabled"]) or s["delivery_mode"] == "paused"
            text += f"  {'⏸' if paused else '✅'} *{s.get('name','Unnamed')}* ({s['delivery_mode']})\n"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏸ Pause All", callback_data="set_pause"),
         InlineKeyboardButton("▶️ Resume All", callback_data="set_resume")],
        [InlineKeyboardButton("🔙 Menu", callback_data="menu:main")],
    ])
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id, text, reply_markup=kb)


async def _handle_pause(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from packages.storage.models import DeliveryMode
    from packages.storage.subscriptions import get_subscriptions_for_workspace, set_delivery_mode
    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id, enabled_only=True)
    for s in subs:
        if s["delivery_mode"] != "paused":
            set_delivery_mode(s["id"], DeliveryMode.PAUSED)
    await _handle_settings(query, context)


async def _handle_resume(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from packages.storage.models import DeliveryMode
    from packages.storage.subscriptions import get_subscriptions_for_workspace, set_delivery_mode
    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id)
    for s in subs:
        if s["delivery_mode"] == "paused":
            set_delivery_mode(s["id"], DeliveryMode.INSTANT)
    await _handle_settings(query, context)


# ═══════════════════════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════════════════════


async def _handle_stats(query, context) -> None:
    from services.bot.handlers.stats import _gather_stats
    stats = _gather_stats()
    lines = ["📈 *Pipeline Statistics*\n",
             f"🏠 Active listings: *{stats['active_listings']}*",
             f"🌐 Sources: *{stats['sources']}*"]
    if stats.get("outbox_stats"):
        o = stats["outbox_stats"]
        lines += ["", "*Outbox:*",
                   f"  Pending: {o.get('pending',0)}",
                   f"  Delivered: {o.get('delivered',0)}",
                   f"  Failed: {o.get('failed',0)}"]
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu:main")]])
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            "\n".join(lines), reply_markup=kb)


# ═══════════════════════════════════════════════════════════════════════════════
# FILTERS MENU
# ═══════════════════════════════════════════════════════════════════════════════


async def _handle_filters(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    f = sub["filters"] if sub else {}

    mx = f.get("max_budget") or f.get("max_price")
    mn = f.get("min_budget") or f.get("min_price")
    pi = "✅" if (mx or mn) else "❌"
    if mn and mx:
        pl = f"€{int(mn):,}–€{int(mx):,}"
    elif mx:
        pl = f"≤€{int(mx):,}"
    elif mn:
        pl = f"≥€{int(mn):,}"
    else:
        pl = "any"

    mb = f.get("min_beds")
    bi = "✅" if mb and int(mb) > 0 else "❌"
    bl = f"{mb}+" if mb else "any"

    cs = f.get("counties", [])
    ci = "✅" if cs else "❌"
    cl = f"{len(cs)} selected" if cs else "all"

    ct = f.get("cities", [])
    ti = "✅" if ct else "❌"
    tl = f"{len(ct)} selected" if ct else "all"

    text = (f"🔍 *Filters*\n\n"
            f"{pi} Price: {pl}\n"
            f"{bi} Beds: {bl}\n"
            f"{ci} County: {cl}\n"
            f"{ti} City: {tl}\n")

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{pi} 💰 Price", callback_data="filter_price")],
        [InlineKeyboardButton(f"{bi} 🛏 Beds", callback_data="filter_beds")],
        [InlineKeyboardButton(f"{ci} 📍 County", callback_data="filter_county:1")],
        [InlineKeyboardButton("🔙 Menu", callback_data="menu:main")],
    ])
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id, text, reply_markup=kb)


# ── Price: main menu ─────────────────────────────────────────────────────

async def _handle_filter_price_prompt(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    f = sub["filters"] if sub else {}
    mx = f.get("max_budget") or f.get("max_price")
    mn = f.get("min_budget") or f.get("min_price")
    if mn and mx:
        cur = f"Current: €{int(mn):,}–€{int(mx):,}"
    elif mx:
        cur = f"Current: ≤€{int(mx):,}"
    elif mn:
        cur = f"Current: ≥€{int(mn):,}"
    else:
        cur = "No filter set"

    rows = [
        [InlineKeyboardButton("💰 Max: 500–1000", callback_data="filter_price_range:500:1000")],
        [InlineKeyboardButton("💰 Max: 1100–2000", callback_data="filter_price_range:1100:2000")],
        [InlineKeyboardButton("💰 Max: 2100–3000", callback_data="filter_price_range:2100:3000")],
        [InlineKeyboardButton("🔀 Custom range (500–10000)", callback_data="filter_price_custom")],
        [InlineKeyboardButton("🗑 Clear", callback_data="filter_price_clear")],
        [InlineKeyboardButton("🔙 Filters", callback_data="filters")],
    ]

    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            f"💰 *Price Filter*\n\n{cur}\n\n"
                            f"*Max budget* — pick a max price:\n"
                            f"*Custom range* — set from–to range:",
                            reply_markup=InlineKeyboardMarkup(rows))


# ── Price: max budget sub-range ───────────────────────────────────────────

async def _handle_filter_price_range(query, context, parts) -> None:
    lo = int(parts[1]) if len(parts) > 1 else 500
    hi = int(parts[2]) if len(parts) > 2 else 1000
    prices = list(range(lo, hi + 1, 100))
    rows = []
    for i in range(0, len(prices), 4):
        rows.append([InlineKeyboardButton(f"€{p:,}",
                     callback_data=f"filter_price_set:{p}")
                     for p in prices[i:i+4]])
    rows.append([InlineKeyboardButton("🔙 Price", callback_data="filter_price")])
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            f"💰 *Max Budget* — €{lo:,}–€{hi:,}\n\nPick max price:",
                            reply_markup=InlineKeyboardMarkup(rows))


# ── Price: custom FROM → TO picker ───────────────────────────────────────

async def _handle_filter_price_custom(query, context) -> None:
    """Step 1: pick FROM price."""
    rows = []
    for i in range(0, len(_PRICE_STEPS), 4):
        rows.append([InlineKeyboardButton(f"€{p:,}",
                     callback_data=f"filter_price_from:{p}")
                     for p in _PRICE_STEPS[i:i+4]])
    rows.append([InlineKeyboardButton("🔙 Price", callback_data="filter_price")])
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            "🔀 *Custom Range*\n\nStep 1/2 — select *FROM* price:",
                            reply_markup=InlineKeyboardMarkup(rows))


async def _handle_filter_price_from(query, context, parts) -> None:
    """Step 2: pick TO price (only prices > from_val)."""
    from_val = int(parts[1]) if len(parts) > 1 else 500
    to_prices = [p for p in _PRICE_STEPS if p > from_val]
    if not to_prices:
        to_prices = [from_val + 500]
    rows = []
    for i in range(0, len(to_prices), 4):
        rows.append([InlineKeyboardButton(f"€{p:,}",
                     callback_data=f"filter_price_fromto:{from_val}:{p}")
                     for p in to_prices[i:i+4]])
    rows.append([InlineKeyboardButton("🔙 Pick FROM", callback_data="filter_price_custom")])
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            f"🔀 *Custom Range*\n\nFROM: *€{from_val:,}*\n\n"
                            f"Step 2/2 — select *TO* price:",
                            reply_markup=InlineKeyboardMarkup(rows))


async def _handle_filter_price_fromto(query, context, parts) -> None:
    lo = int(parts[1]) if len(parts) > 1 else 500
    hi = int(parts[2]) if len(parts) > 2 else 1000
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from packages.storage.subscriptions import update_subscription
    filters = dict(sub["filters"] or {})
    filters["min_budget"] = lo
    filters["max_budget"] = hi
    filters.pop("min_price", None)
    filters.pop("max_price", None)
    update_subscription(sub["id"], filters=filters)
    await _handle_filters(query, context)


async def _handle_filter_price_set(query, context, parts) -> None:
    try:
        price = int(parts[1])
        assert price > 0
    except (ValueError, IndexError, AssertionError):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Retry", callback_data="filter_price")]])
        await edit_menu_message(context, query.message.chat_id,
                                query.message.message_id, "❌ Invalid price.", reply_markup=kb)
        return
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from packages.storage.subscriptions import update_subscription
    filters = dict(sub["filters"] or {})
    filters["max_budget"] = price
    filters.pop("max_price", None)
    filters.pop("min_budget", None)
    filters.pop("min_price", None)
    update_subscription(sub["id"], filters=filters)
    await _handle_filters(query, context)


async def _handle_filter_price_clear(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from packages.storage.subscriptions import update_subscription
    filters = dict(sub["filters"] or {})
    for k in ("max_budget", "max_price", "min_price", "min_budget"):
        filters.pop(k, None)
    update_subscription(sub["id"], filters=filters)
    await _handle_filters(query, context)


# ── Beds ──────────────────────────────────────────────────────────────────

async def _handle_filter_beds_prompt(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    f = sub["filters"] if sub else {}
    mb = f.get("min_beds")
    cur = f"Current: {mb}+ beds" if mb else "No filter set"
    rows = [
        [InlineKeyboardButton(f"{n} bed{'s' if n>1 else ''}", callback_data=f"filter_beds_set:{n}")
         for n in range(1, 5)],
        [InlineKeyboardButton(f"{n} beds", callback_data=f"filter_beds_set:{n}")
         for n in range(5, 8)],
        [InlineKeyboardButton("🗑 Clear", callback_data="filter_beds_clear")],
        [InlineKeyboardButton("🔙 Filters", callback_data="filters")],
    ]
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            f"🛏 *Beds*\n\n{cur}\n\nWARNING\nSome landlords don't write beds amount\n\nSelect min beds:",
                            reply_markup=InlineKeyboardMarkup(rows))


async def _handle_filter_beds_set(query, context, parts) -> None:
    try:
        beds = int(parts[1])
        assert 0 <= beds <= 10
    except (ValueError, IndexError, AssertionError):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Retry", callback_data="filter_beds")]])
        await edit_menu_message(context, query.message.chat_id,
                                query.message.message_id, "❌ Invalid.", reply_markup=kb)
        return
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from packages.storage.subscriptions import update_subscription
    filters = dict(sub["filters"] or {})
    if beds == 0:
        filters.pop("min_beds", None)
    else:
        filters["min_beds"] = beds
    update_subscription(sub["id"], filters=filters)
    await _handle_filters(query, context)


async def _handle_filter_beds_clear(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from packages.storage.subscriptions import update_subscription
    filters = dict(sub["filters"] or {})
    filters.pop("min_beds", None)
    update_subscription(sub["id"], filters=filters)
    await _handle_filters(query, context)


# ── County (8 per page + Select All) ─────────────────────────────────────

COUNTY_PER_PAGE = 8


async def _handle_filter_county(query, context, parts) -> None:
    page = int(parts[1]) if len(parts) > 1 else 1
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    f = sub["filters"] if sub else {}
    selected = [c.lower() for c in f.get("counties", [])]

    tp = max(1, (len(VALID_COUNTIES) + COUNTY_PER_PAGE - 1) // COUNTY_PER_PAGE)
    page = min(page, tp)
    start = (page - 1) * COUNTY_PER_PAGE
    page_c = VALID_COUNTIES[start:start + COUNTY_PER_PAGE]

    buttons = []
    for c in page_c:
        icon = "✅" if c.lower() in selected else "❌"
        buttons.append([
            InlineKeyboardButton(f"{icon} {c}",
                                 callback_data=f"county_toggle:{c}:{page}"),
            InlineKeyboardButton("🏙",
                                 callback_data=f"city_list:{c}:1"),
        ])

    # Pagination
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("◀️", callback_data=f"filter_county:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{tp}", callback_data="noop"))
    if page < tp: nav.append(InlineKeyboardButton("▶️", callback_data=f"filter_county:{page+1}"))
    if nav: buttons.append(nav)

    # Select All / Deselect All
    all_selected = len(selected) == len(VALID_COUNTIES)
    if all_selected:
        buttons.append([InlineKeyboardButton("❌ Deselect All",
                                              callback_data=f"county_all:off:{page}")])
    else:
        buttons.append([InlineKeyboardButton("✅ Select All",
                                              callback_data=f"county_all:on:{page}")])

    buttons.append([InlineKeyboardButton("🔍 Filters", callback_data="filters")])
    buttons.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])

    n = len(selected)
    text = f"📍 *Counties* ({n} selected)\n\nTap county to toggle. Tap 🏙 for cities:"
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id, text,
                            reply_markup=InlineKeyboardMarkup(buttons))


async def _handle_county_toggle(query, context, parts) -> None:
    county = parts[1] if len(parts) > 1 else ""
    page = parts[2] if len(parts) > 2 else "1"
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from packages.storage.subscriptions import update_subscription
    filters = dict(sub["filters"] or {})
    counties = list(filters.get("counties", []))
    low = county.lower()
    existing = [c.lower() for c in counties]
    if low in existing:
        counties = [c for c in counties if c.lower() != low]
    else:
        counties.append(county.title())
    filters["counties"] = counties
    update_subscription(sub["id"], filters=filters)
    await _handle_filter_county(query, context, ["filter_county", page])


async def _handle_county_all(query, context, parts) -> None:
    """Select All or Deselect All counties."""
    action = parts[1] if len(parts) > 1 else "on"
    page = parts[2] if len(parts) > 2 else "1"
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from packages.storage.subscriptions import update_subscription
    filters = dict(sub["filters"] or {})
    if action == "on":
        filters["counties"] = [c.title() for c in VALID_COUNTIES]
    else:
        filters["counties"] = []
    update_subscription(sub["id"], filters=filters)
    await _handle_filter_county(query, context, ["filter_county", page])


# ── City filter (dynamic from pub.city_stats) ─────────────────────────────

CITY_PER_PAGE = 8


async def _handle_city_list(query, context, parts) -> None:
    """Show cities for a county with listing counts."""
    county = parts[1] if len(parts) > 1 else ""
    page = int(parts[2]) if len(parts) > 2 else 1

    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    f = sub["filters"] if sub else {}
    selected_cities = [c.lower() for c in f.get("cities", [])]

    from services.bot.queries.city_stats import get_cities_for_county
    cities = get_cities_for_county(county)

    if not cities:
        buttons = [
            [InlineKeyboardButton("📍 No cities found", callback_data="noop")],
            [InlineKeyboardButton("🔙 Counties", callback_data="filter_county:1")],
        ]
        await edit_menu_message(context, query.message.chat_id,
                                query.message.message_id,
                                f"📍 *{county}* — no cities with active listings",
                                reply_markup=InlineKeyboardMarkup(buttons))
        return

    tp = max(1, (len(cities) + CITY_PER_PAGE - 1) // CITY_PER_PAGE)
    page = min(page, tp)
    start = (page - 1) * CITY_PER_PAGE
    page_cities = cities[start:start + CITY_PER_PAGE]

    buttons = []
    for c in page_cities:
        icon = "✅" if c["city"].lower() in selected_cities else "❌"
        cnt = "9+" if c["count"] > 9 else str(c["count"])
        buttons.append([InlineKeyboardButton(
            f'{icon} {c["city"]} ({cnt})',
            callback_data=f'city_toggle:{county}:{c["city"]}:{page}')])

    # Pagination
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"city_list:{county}:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{tp}", callback_data="noop"))
    if page < tp:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"city_list:{county}:{page+1}"))
    if nav:
        buttons.append(nav)

    # Clear city filter
    if selected_cities:
        buttons.append([InlineKeyboardButton(
            "🗑 Clear city filter", callback_data=f"city_clear:{county}:{page}")])

    buttons.append([InlineKeyboardButton("🔙 Counties", callback_data="filter_county:1")])
    buttons.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])

    n = sum(1 for c in cities if c["city"].lower() in selected_cities)
    txt = f"📍 *Cities in {county}* ({len(cities)} cities)\n"
    if n:
        txt += f"_{n} selected_\n"
    txt += "\nTap to filter by city:"
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id, txt,
                            reply_markup=InlineKeyboardMarkup(buttons))


async def _handle_city_toggle(query, context, parts) -> None:
    """Toggle a city filter on/off."""
    county = parts[1] if len(parts) > 1 else ""
    city = parts[2] if len(parts) > 2 else ""
    page = parts[3] if len(parts) > 3 else "1"

    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from packages.storage.subscriptions import update_subscription
    filters = dict(sub["filters"] or {})
    cities_list = list(filters.get("cities", []))
    low = city.lower()
    existing = [c.lower() for c in cities_list]
    if low in existing:
        cities_list = [c for c in cities_list if c.lower() != low]
    else:
        cities_list.append(city.title())
    filters["cities"] = cities_list
    update_subscription(sub["id"], filters=filters)
    await _handle_city_list(query, context, ["city_list", county, page])


async def _handle_city_clear(query, context, parts) -> None:
    """Clear all city filters."""
    county = parts[1] if len(parts) > 1 else ""
    page = parts[2] if len(parts) > 2 else "1"

    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from packages.storage.subscriptions import update_subscription
    filters = dict(sub["filters"] or {})
    filters.pop("cities", None)
    update_subscription(sub["id"], filters=filters)
    await _handle_city_list(query, context, ["city_list", county, page])

