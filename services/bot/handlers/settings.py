"""
AGPARS Settings Handler

Admin settings for templates, filters, and city configuration.

Covers T042.
"""

from packages.observability.logger import get_logger
from packages.storage.subscriptions import (
    get_subscriptions_for_workspace,
    update_subscription,
)
from services.bot.middleware.auth import AuthContext, require_admin, require_workspace
from services.bot.validation import parse_filter_command

logger = get_logger(__name__)


@require_workspace
async def handle_settings(update, context, auth_ctx: AuthContext) -> None:
    """
    /settings — Show current workspace settings.
    """
    workspace = auth_ctx.workspace
    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id, enabled_only=False)

    settings_text = (
        f"⚙️ *Workspace Settings*\n\n"
        f"Name: *{workspace.title or 'Untitled'}*\n"
        f"Timezone: {workspace.timezone}\n"
        f"Active: {'✅' if workspace.is_active else '❌'}\n\n"
    )

    if subs:
        settings_text += "*Subscriptions:*\n"
        for sub in subs:
            is_paused = (not sub["is_enabled"]) or sub["delivery_mode"] == "paused"
            status = "⏸️" if is_paused else "✅"
            mode = sub["delivery_mode"]
            name = sub.get("name", "Unnamed")
            filters = sub.get("filters", {})

            filter_parts = []
            if filters.get("max_budget"):
                filter_parts.append(f"≤€{filters['max_budget']}")
            if filters.get("min_beds"):
                filter_parts.append(f"{filters['min_beds']}+ beds")
            if filters.get("counties"):
                filter_parts.append(", ".join(filters["counties"]))

            filter_str = " | ".join(filter_parts) if filter_parts else "all"
            settings_text += f"  {status} *{name}* ({mode}) — {filter_str}\n"
    else:
        settings_text += "_No subscriptions configured._\n"

    settings_text += (
        "\n*Edit:*\n"
        "/filter `price:500-2000 beds:2+ county:dublin`\n"
    )

    await update.message.reply_text(settings_text, parse_mode="Markdown")


@require_workspace
@require_admin
async def handle_filter(update, context, auth_ctx: AuthContext) -> None:
    """
    /filter — Set subscription filters.

    Usage: /filter price:500-2000 beds:2+ county:dublin
    """
    text = update.message.text or ""

    # Parse filters from command text
    parsed = parse_filter_command(text)

    if not parsed:
        await update.message.reply_text(
            "📝 *Filter Usage:*\n\n"
            "`/filter price:500-2000` — budget range\n"
            "`/filter beds:2+` — minimum beds\n"
            "`/filter county:dublin,cork` — counties\n"
            "`/filter price:500-2000 beds:1+ county:dublin`\n\n"
            "Combine multiple filters in one command.",
            parse_mode="Markdown",
        )
        return

    # Build filters dict for subscription
    filters = {}
    try:
        if "min_price" in parsed or "min_budget" in parsed:
            filters["min_budget"] = int(parsed.get("min_price", parsed.get("min_budget", 0)))
        if "max_price" in parsed or "max_budget" in parsed:
            filters["max_budget"] = int(parsed.get("max_price", parsed.get("max_budget", 50000)))
        if "min_beds" in parsed:
            filters["min_beds"] = int(parsed["min_beds"])
        if "max_beds" in parsed:
            filters["max_beds"] = int(parsed["max_beds"])
        if "beds" in parsed:
            filters["min_beds"] = int(parsed["beds"])
            filters["max_beds"] = int(parsed["beds"])
    except (ValueError, TypeError):
        await update.message.reply_text(
            "❌ Invalid filter values. Use numbers only.\n\n"
            "Examples:\n"
            "`/filter price:500-2000`\n"
            "`/filter beds:2+`",
            parse_mode="Markdown",
        )
        return
    if "counties" in parsed:
        filters["counties"] = parsed["counties"]
    if "cities" in parsed:
        filters["city_names"] = parsed["cities"]
    if "property_types" in parsed:
        filters["property_types"] = parsed["property_types"]

    # Update first active subscription (or create one)
    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id, enabled_only=True)
    if subs:
        sub = subs[0]
        # Merge with existing filters
        existing_filters = sub.get("filters", {})
        existing_filters.update(filters)
        update_subscription(sub["id"], filters=existing_filters)
        sub_name = sub.get("name", "Default")
    else:
        from packages.storage.subscriptions import create_subscription
        create_subscription(
            workspace_id=auth_ctx.workspace_id,
            name="Custom filter",
            filters=filters,
        )
        sub_name = "Custom filter"

    # Confirm
    filter_summary = []
    if filters.get("min_budget") or filters.get("max_budget"):
        min_b = filters.get("min_budget", "0")
        max_b = filters.get("max_budget", "∞")
        filter_summary.append(f"💰 €{min_b}–€{max_b}")
    if filters.get("min_beds"):
        filter_summary.append(f"🛏 {filters['min_beds']}+ beds")
    if filters.get("counties"):
        filter_summary.append(f"📍 {', '.join(filters['counties'])}")

    await update.message.reply_text(
        f"✅ Filters updated for *{sub_name}*:\n\n"
        + "\n".join(filter_summary),
        parse_mode="Markdown",
    )

    logger.info("Filters updated", workspace_id=auth_ctx.workspace_id, filters=filters)
