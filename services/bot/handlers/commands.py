"""
AGPARS Admin Command Routing

Routes admin commands to appropriate handlers.

Covers T041.
"""

from packages.observability.logger import get_logger
from services.bot.middleware.auth import AuthContext, require_authorized, require_admin

logger = get_logger(__name__)


@require_authorized
@require_admin
async def handle_admin(update, context, auth_ctx: AuthContext) -> None:
    """
    /admin command — shows admin panel.
    """
    workspace = auth_ctx.workspace

    admin_text = (
        "⚙️ *Admin Panel*\n\n"
        f"Workspace: *{workspace.get('title', 'Untitled') if workspace else 'Untitled'}*\n"
        f"Chat ID: `{auth_ctx.tg_chat_id}`\n"
        f"Active: {'✅' if workspace and workspace.get('is_active') else '❌'}\n\n"
        "*Admin commands:*\n"
        "/filter — Set subscription filters\n"
        "/settings — Edit workspace settings\n"
        "/pause — Pause notifications\n"
        "/resume — Resume notifications\n"
        "/users — Manage users\n"
    )

    await update.message.reply_text(admin_text, parse_mode="Markdown")


@require_authorized
@require_admin
async def handle_pause(update, context, auth_ctx: AuthContext) -> None:
    """
    /pause — Pause all notifications for this workspace.
    """
    from packages.storage.models import DeliveryMode
    from packages.storage.subscriptions import (
        get_subscriptions_for_workspace,
        set_delivery_mode,
    )

    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id, enabled_only=True)
    paused_count = 0

    for sub in subs:
        if sub["delivery_mode"] != "paused":
            set_delivery_mode(sub["id"], DeliveryMode.PAUSED)
            paused_count += 1

    await update.message.reply_text(
        f"⏸ Notifications paused for {paused_count} subscription(s).\n"
        f"Use /resume to restart.",
    )
    logger.info("Notifications paused", workspace_id=auth_ctx.workspace_id, count=paused_count)


@require_authorized
@require_admin
async def handle_resume(update, context, auth_ctx: AuthContext) -> None:
    """
    /resume — Resume all notifications for this workspace.
    """
    from packages.storage.models import DeliveryMode
    from packages.storage.subscriptions import (
        get_subscriptions_for_workspace,
        set_delivery_mode,
    )

    subs = get_subscriptions_for_workspace(auth_ctx.workspace_id)
    resumed_count = 0

    for sub in subs:
        if sub["delivery_mode"] == "paused":
            set_delivery_mode(sub["id"], DeliveryMode.INSTANT)
            resumed_count += 1

    await update.message.reply_text(
        f"▶️ Notifications resumed for {resumed_count} subscription(s).",
    )
    logger.info("Notifications resumed", workspace_id=auth_ctx.workspace_id, count=resumed_count)
