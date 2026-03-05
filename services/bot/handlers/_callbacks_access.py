"""
AGPARS Access Request Callbacks

Handles: request_access, admin_accept:ID, admin_decline:ID
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from packages.core.config import get_settings
from packages.observability.logger import get_logger
from packages.storage.access_requests import (
    create_access_request,
    get_pending_request_for_user,
    get_request_by_id,
    approve_request,
    decline_request,
    set_admin_message_id,
)
from packages.storage.models import UserRole, WorkspaceType
from packages.storage.subscriptions import create_subscription
from packages.storage.users import get_or_create_user, get_user_by_id, set_user_role
from packages.storage.workspaces import get_or_create_workspace
from services.bot.handlers.callbacks import build_main_menu_keyboard
from services.bot.message_manager import edit_menu_message, send_menu_message

logger = get_logger(__name__)


async def handle_request_access(query, context) -> None:
    """User clicks '📨 Request Access' — create request and notify admin group."""
    tg_user = query.from_user
    settings = get_settings()

    bot_user = get_or_create_user(
        tg_user_id=tg_user.id,
        tg_username=tg_user.username,
        full_name=tg_user.full_name,
    )

    # Check if already authorized
    if bot_user["role"] in (UserRole.REGULAR.value, UserRole.ADMIN.value):
        await edit_menu_message(
            context, query.message.chat_id, query.message.message_id,
            "✅ Вы уже авторизованы! Используйте /start для входа.",
        )
        return

    # Check for pending request
    pending = get_pending_request_for_user(bot_user["id"])
    if pending:
        await edit_menu_message(
            context, query.message.chat_id, query.message.message_id,
            "⏳ Ваш запрос уже отправлен. Ожидайте решения администратора.",
        )
        return

    # Create access request
    req_id = create_access_request(bot_user["id"])

    # Notify admin group
    admin_group_id = settings.telegram.admin_group_id
    username_display = f"@{tg_user.username}" if tg_user.username else tg_user.full_name

    admin_text = (
        f"📨 *Новый запрос доступа*\n\n"
        f"👤 Пользователь: {username_display}\n"
        f"🆔 Telegram ID: `{tg_user.id}`\n"
        f"📝 Request ID: `{req_id}`"
    )

    admin_keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Accept", callback_data=f"admin_accept:{req_id}"),
            InlineKeyboardButton("❌ Decline", callback_data=f"admin_decline:{req_id}"),
        ]
    ])

    try:
        msg = await context.bot.send_message(
            chat_id=admin_group_id,
            text=admin_text,
            parse_mode="Markdown",
            reply_markup=admin_keyboard,
        )
        set_admin_message_id(req_id, msg.message_id)
    except Exception as e:
        logger.error("Failed to send to admin group", error=str(e))

    # Confirm to user
    await edit_menu_message(
        context, query.message.chat_id, query.message.message_id,
        "✅ Запрос отправлен! Ожидайте решения администратора.",
    )

    logger.info("Access request sent", request_id=req_id, user_id=bot_user["id"])


async def handle_admin_accept(query, context, parts) -> None:
    """Admin clicks 'Accept' in admin group."""
    if len(parts) < 2:
        return

    req_id = int(parts[1])
    req = get_request_by_id(req_id)
    if not req:
        await query.answer("Request not found", show_alert=True)
        return

    if req["status"] != "pending":
        await query.answer(f"Already {req['status']}", show_alert=True)
        return

    # Get admin user
    admin_tg = query.from_user
    admin_user = get_or_create_user(
        tg_user_id=admin_tg.id,
        tg_username=admin_tg.username,
        full_name=admin_tg.full_name,
    )

    # Approve request
    approve_request(req_id, reviewed_by_user_id=admin_user["id"])

    # Set user role to regular
    set_user_role(req["user_id"], UserRole.REGULAR)

    # Get user info
    user = get_user_by_id(req["user_id"])
    if not user:
        return

    # Create personal workspace for user
    workspace = get_or_create_workspace(
        workspace_type=WorkspaceType.PERSONAL,
        tg_chat_id=user["tg_user_id"],
        owner_user_id=user["id"],
        title=user.get("full_name") or f"User {user['tg_user_id']}",
    )

    # Create default subscription
    create_subscription(workspace_id=workspace["id"], name="All listings", filters={})

    # Update admin group message
    username = user.get("tg_username")
    display = f"@{username}" if username else user.get("full_name", "Unknown")
    admin_name = admin_tg.full_name or admin_tg.username or str(admin_tg.id)

    await edit_menu_message(
        context, query.message.chat_id, query.message.message_id,
        f"✅ *Доступ одобрен*\n\n"
        f"👤 {display}\n"
        f"📝 Request: `{req_id}`\n"
        f"👮 Approved by: {admin_name}",
    )

    # Notify user
    try:
        keyboard = build_main_menu_keyboard()
        await context.bot.send_message(
            chat_id=user["tg_user_id"],
            text="✅ *Ваш запрос одобрен!*\n\nДобро пожаловать! Используйте /start для начала.",
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error("Failed to notify user", error=str(e), tg_user_id=user["tg_user_id"])

    logger.info("Access request approved", request_id=req_id, user_id=req["user_id"])


async def handle_admin_decline(query, context, parts) -> None:
    """Admin clicks 'Decline' in admin group."""
    if len(parts) < 2:
        return

    req_id = int(parts[1])
    req = get_request_by_id(req_id)
    if not req:
        await query.answer("Request not found", show_alert=True)
        return

    if req["status"] != "pending":
        await query.answer(f"Already {req['status']}", show_alert=True)
        return

    # Get admin user
    admin_tg = query.from_user
    admin_user = get_or_create_user(
        tg_user_id=admin_tg.id,
        tg_username=admin_tg.username,
        full_name=admin_tg.full_name,
    )

    # Decline request
    decline_request(req_id, reviewed_by_user_id=admin_user["id"])

    user = get_user_by_id(req["user_id"])
    if not user:
        return

    username = user.get("tg_username")
    display = f"@{username}" if username else user.get("full_name", "Unknown")
    admin_name = admin_tg.full_name or admin_tg.username or str(admin_tg.id)

    # Update admin group message
    await edit_menu_message(
        context, query.message.chat_id, query.message.message_id,
        f"❌ *Доступ отклонён*\n\n"
        f"👤 {display}\n"
        f"📝 Request: `{req_id}`\n"
        f"👮 Declined by: {admin_name}",
    )

    # Notify user
    try:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📨 Запросить повторно", callback_data="request_access")]
        ])
        await context.bot.send_message(
            chat_id=user["tg_user_id"],
            text="❌ *Ваш запрос отклонён.*\n\nВы можете отправить повторный запрос.",
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error("Failed to notify user", error=str(e), tg_user_id=user["tg_user_id"])

    logger.info("Access request declined", request_id=req_id, user_id=req["user_id"])
