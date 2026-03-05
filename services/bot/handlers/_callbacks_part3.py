"""
AGPARS Callback Handlers Part 3

Digest menu with dynamic Pause/Resume, 2-step time pickers.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from packages.observability.logger import get_logger
from services.bot.message_manager import edit_menu_message
from services.bot.handlers._callback_helpers import (
    PAGE_SIZE, safe_price, get_auth, get_sub, validate_time, DAYS_OF_WEEK,
)

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DIGEST MENU
# ═══════════════════════════════════════════════════════════════════════════════


def _digest_status(sub: dict | None) -> str:
    if not sub:
        return "⚠️ No subscription."
    mode = sub.get("delivery_mode", "instant")
    sched = sub.get("digest_schedule", {}) or {}
    if mode == "paused":
        return "⏸ *Status*: Paused"
    elif mode == "instant":
        return "▶️ *Status*: Instant delivery"
    elif mode == "digest":
        freq = sched.get("frequency", "daily")
        t = sched.get("time", "09:00")
        if freq == "daily":
            return f"📬 *Status*: Daily at {t}"
        elif freq == "weekly":
            d = sched.get("day_of_week", "monday")
            return f"📬 *Status*: Weekly {d.title()} at {t}"
        return f"📬 *Status*: Digest ({freq})"
    return f"❓ *Status*: {mode}"


async def _handle_digest_menu(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    is_paused = sub and sub.get("delivery_mode") == "paused"
    text = f"📬 *Digest Settings*\n\n{_digest_status(sub)}\n\nChoose mode:"

    rows = [
        [InlineKeyboardButton("📨 Instant delivery", callback_data="digest_off")],
        [InlineKeyboardButton("📅 Daily digest", callback_data="digest_daily")],
        [InlineKeyboardButton("📆 Weekly digest", callback_data="digest_weekly")],
        [InlineKeyboardButton("📬 Quick: daily 18:00", callback_data="digest_on")],
    ]
    # Dynamic Pause/Resume
    if is_paused:
        rows.append([InlineKeyboardButton("▶️ Resume", callback_data="digest_resume")])
    else:
        rows.append([InlineKeyboardButton("⏸ Pause", callback_data="digest_pause")])
    rows.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])

    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id, text,
                            reply_markup=InlineKeyboardMarkup(rows))


async def _handle_digest_on(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from services.notifier.delivery_mode import switch_to_digest
    switch_to_digest(sub["id"], {"frequency": "daily", "time": "18:00", "timezone": "Europe/Dublin"})
    # Redirect back to digest menu (shows updated status)
    await _handle_digest_menu(query, context)


async def _handle_digest_off(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from services.notifier.delivery_mode import switch_to_instant
    switch_to_instant(sub["id"])
    await _handle_digest_menu(query, context)


async def _handle_digest_pause(query, context) -> None:
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from services.notifier.delivery_mode import switch_to_paused
    switch_to_paused(sub["id"])
    await _handle_digest_menu(query, context)


async def _handle_digest_resume(query, context) -> None:
    """Resume = switch back to instant delivery."""
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from services.notifier.delivery_mode import switch_to_instant
    switch_to_instant(sub["id"])
    await _handle_digest_menu(query, context)


# ── Daily: 2-step time picker ────────────────────────────────────────────

async def _handle_digest_daily_prompt(query, context) -> None:
    """Step 1: show time range buttons."""
    rows = [
        [InlineKeyboardButton("🕐 1:00 – 12:00", callback_data="digest_daily_range:1")],
        [InlineKeyboardButton("🕐 13:00 – 23:00", callback_data="digest_daily_range:13")],
        [InlineKeyboardButton("🔙 Digest", callback_data="digest_menu")],
    ]
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            "📅 *Daily Digest*\n\nPick a time range:",
                            reply_markup=InlineKeyboardMarkup(rows))


async def _handle_digest_daily_range(query, context, parts) -> None:
    """Step 2: show individual hours in the selected range."""
    start_h = int(parts[1]) if len(parts) > 1 else 1
    if start_h == 1:
        hours = list(range(1, 13))
    else:
        hours = list(range(13, 24))
    rows = []
    for i in range(0, len(hours), 4):
        rows.append([InlineKeyboardButton(f"{h:02d}:00",
                     callback_data=f"digest_set_time:{h:02d}:00")
                     for h in hours[i:i+4]])
    rows.append([InlineKeyboardButton("🔙 Pick range", callback_data="digest_daily")])
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            f"📅 *Daily Digest* — pick time:",
                            reply_markup=InlineKeyboardMarkup(rows))


async def _handle_digest_set_time(query, context, parts) -> None:
    t = f"{parts[1]}:{parts[2]}" if len(parts) > 2 else parts[1] if len(parts) > 1 else "09:00"
    if not validate_time(t):
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Try again", callback_data="digest_daily")]])
        await edit_menu_message(context, query.message.chat_id,
                                query.message.message_id,
                                f"❌ Invalid time.", reply_markup=kb)
        return
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from services.notifier.delivery_mode import switch_to_digest
    switch_to_digest(sub["id"], {"frequency": "daily", "time": t, "timezone": "Europe/Dublin"})
    await _handle_digest_menu(query, context)


# ── Weekly: day → 2-step time ────────────────────────────────────────────

async def _handle_digest_weekly_days(query, context) -> None:
    rows = [[InlineKeyboardButton(f"📆 {n}", callback_data=f"digest_weekly_day:{k}")]
            for n, k in DAYS_OF_WEEK]
    rows.append([InlineKeyboardButton("🔙 Digest", callback_data="digest_menu")])
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            "📆 *Weekly Digest*\n\nPick a day:",
                            reply_markup=InlineKeyboardMarkup(rows))


async def _handle_digest_weekly_day(query, context, parts) -> None:
    """After picking day, show time range buttons (same 2-step as daily)."""
    day = parts[1] if len(parts) > 1 else "monday"
    rows = [
        [InlineKeyboardButton("🕐 1:00 – 12:00", callback_data=f"digest_weekly_range:{day}:1")],
        [InlineKeyboardButton("🕐 13:00 – 23:00", callback_data=f"digest_weekly_range:{day}:13")],
        [InlineKeyboardButton("🔙 Pick day", callback_data="digest_weekly")],
    ]
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            f"📆 *{day.title()}* — pick time range:",
                            reply_markup=InlineKeyboardMarkup(rows))


async def _handle_digest_weekly_range(query, context, parts) -> None:
    """Show individual hours in range, for selected day."""
    day = parts[1] if len(parts) > 1 else "monday"
    start_h = int(parts[2]) if len(parts) > 2 else 1
    if start_h == 1:
        hours = list(range(1, 13))
    else:
        hours = list(range(13, 24))
    rows = []
    for i in range(0, len(hours), 4):
        rows.append([InlineKeyboardButton(f"{h:02d}:00",
                     callback_data=f"digest_weekly_time:{day}:{h:02d}:00")
                     for h in hours[i:i+4]])
    rows.append([InlineKeyboardButton("🔙 Pick range", callback_data=f"digest_weekly_day:{day}")])
    await edit_menu_message(context, query.message.chat_id,
                            query.message.message_id,
                            f"📆 *{day.title()}* — pick time:",
                            reply_markup=InlineKeyboardMarkup(rows))


async def _handle_digest_weekly_time(query, context, parts) -> None:
    day = parts[1] if len(parts) > 1 else "monday"
    t = f"{parts[2]}:{parts[3]}" if len(parts) > 3 else "09:00"
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    sub = get_sub(auth_ctx)
    if not sub:
        return
    from services.notifier.delivery_mode import switch_to_digest
    switch_to_digest(sub["id"], {"frequency": "weekly", "day_of_week": day,
                                  "time": t, "timezone": "Europe/Dublin"})
    await _handle_digest_menu(query, context)


# ── Digest events page ──────────────────────────────────────────────────

async def _handle_digest_page(query, context, parts) -> None:
    page = int(parts[1]) if len(parts) > 1 else 1
    auth_ctx = get_auth(query)
    if not auth_ctx or not auth_ctx.has_workspace:
        return
    from packages.telegram.digest import get_pending_events_for_workspace
    events = get_pending_events_for_workspace(auth_ctx.workspace_id)
    if not events:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu:main")]])
        await edit_menu_message(context, query.message.chat_id,
                                query.message.message_id,
                                "📬 No pending events.", reply_markup=kb)
        return
    total = len(events)
    tp = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, tp)
    start = (page - 1) * PAGE_SIZE
    lines = [f"📬 *Digest* — {page}/{tp} ({total} total)\n"]
    for i, ev in enumerate(events[start:start+PAGE_SIZE], start=start+1):
        pl = ev.get("payload", {})
        lines.append(f"{i}. {safe_price(pl.get('price'))} | {pl.get('beds') or '?'}BR | {pl.get('city','?')}")
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("◀️", callback_data=f"digest_page:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{tp}", callback_data="noop"))
    if page < tp: nav.append(InlineKeyboardButton("▶️", callback_data=f"digest_page:{page+1}"))
    buttons = [nav, [InlineKeyboardButton("🔙 Menu", callback_data="menu:main")]]
    await edit_menu_message(context, query.message.chat_id, query.message.message_id,
                            "\n".join(lines), reply_markup=InlineKeyboardMarkup(buttons))
