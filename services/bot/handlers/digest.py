"""
AGPARS Digest Admin Commands

/digest — Configure digest delivery mode and schedule.

Covers T072.
"""

from packages.observability.logger import get_logger
from services.bot.middleware.auth import AuthContext, require_admin, require_workspace
from services.notifier.delivery_mode import switch_to_digest, switch_to_instant, switch_to_paused

logger = get_logger(__name__)


@require_workspace
@require_admin
async def handle_digest(update, context, auth_ctx: AuthContext) -> None:
    """
    /digest [on|off|daily|weekly|twice_daily] — Configure digest mode.

    Usage:
        /digest on          → Enable daily digest (09:00)
        /digest off         → Switch to instant delivery
        /digest daily 18:00 → Daily at 18:00
        /digest weekly mon  → Weekly on Monday at 09:00
        /digest twice_daily → Twice daily (09:00 + 18:00)
        /digest pause       → Pause all notifications
    """
    from packages.storage.subscriptions import get_subscriptions_for_workspace

    args = (update.message.text or "").split()
    action = args[1].lower() if len(args) > 1 else "status"

    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id, enabled_only=True)
    if not subs:
        await update.message.reply_text("⚠️ No active subscriptions to configure.")
        return

    sub = subs[0]
    sub_id = sub["id"]

    if action == "status":
        mode = sub.get("delivery_mode", "instant")
        schedule = sub.get("digest_schedule", {})
        await update.message.reply_text(
            f"📬 *Delivery Mode*: {mode}\n"
            f"Schedule: {_format_schedule(schedule) if schedule else 'N/A'}\n\n"
            "*Commands:*\n"
            "`/digest on` — daily digest\n"
            "`/digest off` — instant mode\n"
            "`/digest daily 18:00` — daily at 18:00\n"
            "`/digest weekly mon` — weekly on Monday\n"
            "`/digest pause` — pause notifications",
            parse_mode="Markdown",
        )
        return

    if action == "off":
        switch_to_instant(sub_id)
        await update.message.reply_text("▶️ Switched to *instant* delivery mode.", parse_mode="Markdown")
        return

    if action == "pause":
        switch_to_paused(sub_id)
        await update.message.reply_text("⏸ Notifications paused. Use `/digest on` to resume.", parse_mode="Markdown")
        return

    if action == "on" or action == "daily":
        time_str = args[2] if len(args) > 2 else "09:00"
        schedule = {"frequency": "daily", "time": time_str, "timezone": "Europe/Dublin"}
        switch_to_digest(sub_id, schedule)
        await update.message.reply_text(
            f"📬 Digest mode: *daily at {time_str}*",
            parse_mode="Markdown",
        )
        return

    if action == "weekly":
        day = args[2] if len(args) > 2 else "monday"
        time_str = args[3] if len(args) > 3 else "09:00"
        schedule = {
            "frequency": "weekly",
            "day_of_week": day,
            "time": time_str,
            "timezone": "Europe/Dublin",
        }
        switch_to_digest(sub_id, schedule)
        await update.message.reply_text(
            f"📬 Digest mode: *weekly on {day.title()} at {time_str}*",
            parse_mode="Markdown",
        )
        return

    if action == "twice_daily":
        schedule = {"frequency": "twice_daily", "hours": [9, 18], "timezone": "Europe/Dublin"}
        switch_to_digest(sub_id, schedule)
        await update.message.reply_text(
            "📬 Digest mode: *twice daily* (09:00 + 18:00)",
            parse_mode="Markdown",
        )
        return

    await update.message.reply_text(
        f"❓ Unknown action: `{action}`. Use `/digest` for help.",
        parse_mode="Markdown",
    )


def _format_schedule(schedule: dict) -> str:
    """Format schedule dict for display."""
    freq = schedule.get("frequency", "daily")
    time_str = schedule.get("time", "09:00")

    if freq == "daily":
        return f"Daily at {time_str}"
    if freq == "twice_daily":
        hours = schedule.get("hours", [9, 18])
        return f"Twice daily ({':00, '.join(str(h) for h in hours)}:00)"
    if freq == "weekly":
        day_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
        day = schedule.get("day_of_week", 0)
        day_name = day_map.get(day, str(day))
        return f"Weekly on {day_name} at {time_str}"
    return freq
