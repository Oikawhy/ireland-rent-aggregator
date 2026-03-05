"""
AGPARS Onboarding Handler

/start command — role-aware flow with access request for unauthorized users.
/help command — shows emoji button menu.

Covers T040.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from packages.core.config import get_settings
from packages.observability.logger import get_logger
from packages.storage.models import UserRole, WorkspaceType
from packages.storage.subscriptions import create_subscription, get_subscriptions_for_workspace
from packages.storage.users import get_or_create_user
from packages.storage.workspaces import get_or_create_workspace
from services.bot.handlers.callbacks import build_main_menu_keyboard
from services.bot.message_manager import send_menu_message

logger = get_logger(__name__)


async def handle_start(update, context) -> None:
    """
    /start command handler.

    Role-aware flow:
    - Unauthorized users see "Request Access" button
    - First admin (configured by admin_user_id) is auto-promoted
    - Authorized users get workspace + menu
    """
    user = update.effective_user
    chat = update.effective_chat
    settings = get_settings()

    # Auto-admin for configured admin user
    default_role = UserRole.UNAUTHORIZED
    if user.id == settings.telegram.admin_user_id:
        default_role = UserRole.ADMIN

    bot_user = get_or_create_user(
        tg_user_id=user.id,
        tg_username=user.username,
        full_name=user.full_name,
        default_role=default_role,
    )
    role = bot_user["role"]

    # Unauthorized → show request access button
    if role == UserRole.UNAUTHORIZED.value:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📨 Запросить доступ", callback_data="request_access")]
        ])
        await send_menu_message(
            context, chat.id,
            "⛔ *У вас нет доступа*\n\n"
            "Нажмите кнопку ниже, чтобы отправить запрос администратору.",
            reply_markup=keyboard,
        )
        return

    # Authorized → create workspace + show menu
    ws_type = (
        WorkspaceType.GROUP if chat.type in ("group", "supergroup")
        else WorkspaceType.PERSONAL
    )
    title = chat.title or user.full_name

    workspace = get_or_create_workspace(
        workspace_type=ws_type,
        tg_chat_id=chat.id,
        owner_user_id=bot_user["id"],
        title=title,
    )
    workspace_id = workspace["id"]

    # Create default subscription if none exists
    existing_subs = get_subscriptions_for_workspace(workspace_id)
    if not existing_subs:
        create_subscription(workspace_id=workspace_id, name="All listings", filters={})
        sub_msg = "\n📋 Default subscription created (all listings)."
    else:
        sub_msg = f"\n📋 You have {len(existing_subs)} subscription(s)."

    welcome = (
        f"🏠 *Welcome to AGPARS Rental Aggregator!*\n\n"
        f"Workspace: *{title}*\n"
        f"Role: {role.title()}\n"
        f"{sub_msg}\n\n"
        f"Select an option below:"
    )

    keyboard = build_main_menu_keyboard(is_admin=(role == UserRole.ADMIN.value))
    await send_menu_message(context, chat.id, welcome, reply_markup=keyboard)


async def handle_help(update, context) -> None:
    """
    /help command handler — shows emoji button menu.
    """
    chat = update.effective_chat
    user = update.effective_user

    from packages.storage.users import get_user_by_tg_id

    bot_user = get_user_by_tg_id(user.id)
    is_admin = bot_user and bot_user["role"] == UserRole.ADMIN.value

    help_text = (
        "🤖 *AGPARS Rental Aggregator*\n\n"
        "Use the buttons below to navigate:"
    )

    keyboard = build_main_menu_keyboard(is_admin=is_admin)
    await send_menu_message(context, chat.id, help_text, reply_markup=keyboard)
