"""
AGPARS Admin User Management Callbacks

Handles: users_menu, users_page, user_detail, user_role, user_set_role,
         user_delete, user_delete_confirm
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from packages.observability.logger import get_logger
from packages.storage.models import UserRole, WorkspaceType
from packages.storage.subscriptions import create_subscription
from packages.storage.users import (
    delete_user,
    get_user_by_id,
    get_user_by_tg_id,
    list_all_users,
    set_user_role,
)
from packages.storage.workspaces import (
    delete_workspace_by_chat_id,
    get_or_create_workspace,
)
from services.bot.message_manager import edit_menu_message

logger = get_logger(__name__)

USERS_PAGE_SIZE = 8

ROLE_EMOJI = {
    "unauthorized": "🔴",
    "regular": "🟢",
    "admin": "⭐",
}


async def handle_users_menu(query, context, parts) -> None:
    """Show paginated user list — admin only."""
    # Check admin
    caller = get_user_by_tg_id(query.from_user.id)
    if not caller or caller["role"] != UserRole.ADMIN.value:
        await query.answer("Admin only", show_alert=True)
        return

    page = int(parts[1]) if len(parts) > 1 else 1
    users = list_all_users()
    total = len(users)
    total_pages = max(1, (total + USERS_PAGE_SIZE - 1) // USERS_PAGE_SIZE)
    page = min(page, total_pages)
    offset = (page - 1) * USERS_PAGE_SIZE
    page_users = users[offset : offset + USERS_PAGE_SIZE]

    if not page_users:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Menu", callback_data="menu:main")]
        ])
        await edit_menu_message(
            context, query.message.chat_id, query.message.message_id,
            "👥 No users found.", reply_markup=kb,
        )
        return

    buttons = []
    for u in page_users:
        emoji = ROLE_EMOJI.get(u["role"], "❓")
        name = u.get("full_name") or u.get("tg_username") or str(u["tg_user_id"])
        buttons.append([
            InlineKeyboardButton(
                f"{emoji} {name} [{u['role']}]",
                callback_data=f"user_detail:{u['id']}",
            )
        ])

    # Pagination
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"users_page:{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"users_page:{page + 1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton("🔙 Menu", callback_data="menu:main")])

    await edit_menu_message(
        context, query.message.chat_id, query.message.message_id,
        f"👥 *Users* — {total} total",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def handle_user_detail(query, context, parts) -> None:
    """Show user detail card."""
    caller = get_user_by_tg_id(query.from_user.id)
    if not caller or caller["role"] != UserRole.ADMIN.value:
        await query.answer("Admin only", show_alert=True)
        return

    if len(parts) < 2:
        return

    user_id = int(parts[1])
    user = get_user_by_id(user_id)
    if not user:
        await query.answer("User not found", show_alert=True)
        return

    emoji = ROLE_EMOJI.get(user["role"], "❓")
    username = f"@{user['tg_username']}" if user["tg_username"] else "—"
    text = (
        f"👤 *User Detail*\n\n"
        f"Name: *{user.get('full_name') or '—'}*\n"
        f"Username: {username}\n"
        f"TG ID: `{user['tg_user_id']}`\n"
        f"Role: {emoji} {user['role']}\n"
        f"Active: {'✅' if user['is_active'] else '❌'}\n"
        f"Created: {user['created_at']:%Y-%m-%d %H:%M}"
    )

    buttons = [
        [InlineKeyboardButton(
            "🔄 Change Role", callback_data=f"user_role:{user_id}"
        )],
        [InlineKeyboardButton(
            "🗑 Delete User", callback_data=f"user_delete:{user_id}"
        )],
        [InlineKeyboardButton("🔙 Users", callback_data="users_menu")],
        [InlineKeyboardButton("🔙 Menu", callback_data="menu:main")],
    ]

    await edit_menu_message(
        context, query.message.chat_id, query.message.message_id,
        text, reply_markup=InlineKeyboardMarkup(buttons),
    )


async def handle_user_role(query, context, parts) -> None:
    """Show role change options for a user."""
    caller = get_user_by_tg_id(query.from_user.id)
    if not caller or caller["role"] != UserRole.ADMIN.value:
        await query.answer("Admin only", show_alert=True)
        return

    if len(parts) < 2:
        return

    user_id = int(parts[1])
    user = get_user_by_id(user_id)
    if not user:
        await query.answer("User not found", show_alert=True)
        return

    buttons = []
    for role in UserRole:
        emoji = ROLE_EMOJI.get(role.value, "❓")
        current = " ✓" if user["role"] == role.value else ""
        buttons.append([
            InlineKeyboardButton(
                f"{emoji} {role.value.title()}{current}",
                callback_data=f"user_set_role:{user_id}:{role.value}",
            )
        ])

    buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"user_detail:{user_id}")])

    await edit_menu_message(
        context, query.message.chat_id, query.message.message_id,
        f"🔄 *Change role for* `{user.get('full_name') or user['tg_user_id']}`",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def handle_user_set_role(query, context, parts) -> None:
    """Set user role — with self-demotion protection."""
    caller = get_user_by_tg_id(query.from_user.id)
    if not caller or caller["role"] != UserRole.ADMIN.value:
        await query.answer("Admin only", show_alert=True)
        return

    if len(parts) < 3:
        return

    user_id = int(parts[1])
    new_role_str = parts[2]

    try:
        new_role = UserRole(new_role_str)
    except ValueError:
        await query.answer("Invalid role", show_alert=True)
        return

    user = get_user_by_id(user_id)
    if not user:
        await query.answer("User not found", show_alert=True)
        return

    # Self-demotion protection
    if user["tg_user_id"] == query.from_user.id and new_role != UserRole.ADMIN:
        await query.answer("⚠️ Cannot demote yourself!", show_alert=True)
        return

    set_user_role(user_id, new_role)

    # If promoted to regular/admin, ensure workspace exists
    if new_role in (UserRole.REGULAR, UserRole.ADMIN):
        _ensure_workspace(user)

    await query.answer(f"Role set to {new_role.value}")
    # Refresh detail view
    await handle_user_detail(query, context, ["user_detail", str(user_id)])

    logger.info(
        "User role changed by admin",
        target_user_id=user_id,
        new_role=new_role.value,
        admin_tg_id=query.from_user.id,
    )


async def handle_user_delete(query, context, parts) -> None:
    """Show delete confirmation."""
    caller = get_user_by_tg_id(query.from_user.id)
    if not caller or caller["role"] != UserRole.ADMIN.value:
        await query.answer("Admin only", show_alert=True)
        return

    if len(parts) < 2:
        return

    user_id = int(parts[1])
    user = get_user_by_id(user_id)
    if not user:
        await query.answer("User not found", show_alert=True)
        return

    # Self-deletion protection
    if user["tg_user_id"] == query.from_user.id:
        await query.answer("⚠️ Cannot delete yourself!", show_alert=True)
        return

    name = user.get("full_name") or str(user["tg_user_id"])
    buttons = [
        [InlineKeyboardButton(
            "⚠️ Confirm Delete",
            callback_data=f"user_delete_confirm:{user_id}",
        )],
        [InlineKeyboardButton("🔙 Cancel", callback_data=f"user_detail:{user_id}")],
    ]

    await edit_menu_message(
        context, query.message.chat_id, query.message.message_id,
        f"🗑 *Delete user {name}?*\n\n"
        f"This will cascade-delete their workspace, subscriptions, and requests.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def handle_user_delete_confirm(query, context, parts) -> None:
    """Actually delete user with cascade."""
    caller = get_user_by_tg_id(query.from_user.id)
    if not caller or caller["role"] != UserRole.ADMIN.value:
        await query.answer("Admin only", show_alert=True)
        return

    if len(parts) < 2:
        return

    user_id = int(parts[1])
    user = get_user_by_id(user_id)
    if not user:
        await query.answer("User not found", show_alert=True)
        return

    # Self-deletion protection
    if user["tg_user_id"] == query.from_user.id:
        await query.answer("⚠️ Cannot delete yourself!", show_alert=True)
        return

    # Delete workspace first, then user (cascade)
    delete_workspace_by_chat_id(user["tg_user_id"])
    delete_user(user_id)

    await query.answer("User deleted")
    logger.info(
        "User deleted by admin",
        target_user_id=user_id,
        admin_tg_id=query.from_user.id,
    )

    # Return to user list
    await handle_users_menu(query, context, ["users_menu"])


def _ensure_workspace(user: dict) -> None:
    """Create personal workspace for a user if none exists."""
    from packages.storage.workspaces import get_workspace_by_chat_id

    existing = get_workspace_by_chat_id(user["tg_user_id"])
    if existing:
        return

    workspace = get_or_create_workspace(
        workspace_type=WorkspaceType.PERSONAL,
        tg_chat_id=user["tg_user_id"],
        owner_user_id=user["id"],
        title=user.get("full_name") or f"User {user['tg_user_id']}",
    )
    create_subscription(workspace_id=workspace["id"], name="All listings", filters={})
