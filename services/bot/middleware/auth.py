"""
AGPARS Workspace Auth Middleware

Role-based access control using bot.users.
Replaces legacy workspace_admins-based auth.
"""

from collections.abc import Callable
from functools import wraps

from packages.observability.logger import get_logger
from packages.storage.models import UserRole
from packages.storage.users import get_user_by_tg_id

logger = get_logger(__name__)


class AuthContext:
    """Authentication context for a request."""

    def __init__(
        self,
        tg_user_id: int,
        tg_chat_id: int,
        user: dict | None = None,
        workspace: dict | None = None,
    ):
        self.tg_user_id = tg_user_id
        self.tg_chat_id = tg_chat_id
        self.user = user
        self.workspace = workspace

    @property
    def user_id(self) -> int | None:
        return self.user["id"] if self.user else None

    @property
    def role(self) -> str:
        return self.user["role"] if self.user else UserRole.UNAUTHORIZED.value

    @property
    def is_authorized(self) -> bool:
        return self.role in (UserRole.REGULAR.value, UserRole.ADMIN.value)

    @property
    def is_admin(self) -> bool:
        return self.role == UserRole.ADMIN.value

    @property
    def workspace_id(self) -> int | None:
        return self.workspace["id"] if self.workspace else None

    @property
    def has_workspace(self) -> bool:
        return self.workspace is not None

    @property
    def can_configure(self) -> bool:
        """Check if user can modify workspace settings."""
        return self.is_authorized


def get_auth_context(tg_user_id: int, tg_chat_id: int) -> AuthContext:
    """
    Build authentication context for a request.

    Args:
        tg_user_id: Telegram user ID
        tg_chat_id: Telegram chat ID

    Returns:
        AuthContext with user role and workspace info
    """
    from packages.storage.workspaces import get_workspace_by_chat_id

    user = get_user_by_tg_id(tg_user_id)
    workspace = get_workspace_by_chat_id(tg_chat_id)
    return AuthContext(
        tg_user_id=tg_user_id,
        tg_chat_id=tg_chat_id,
        user=user,
        workspace=workspace,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# DECORATORS FOR HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════


def require_authorized(func: Callable) -> Callable:
    """
    Requires user to be authorized (regular or admin).

    Injects AuthContext as first argument after update/context.
    """
    @wraps(func)
    async def wrapper(update, context, *args, **kwargs):
        tg_user_id = update.effective_user.id
        tg_chat_id = update.effective_chat.id

        auth_ctx = get_auth_context(tg_user_id, tg_chat_id)

        if not auth_ctx.is_authorized:
            await update.message.reply_text(
                "⛔ У вас нет доступа к этому боту."
            )
            return

        if not auth_ctx.has_workspace:
            await update.message.reply_text(
                "⚠️ No workspace configured. Use /start to set up."
            )
            return

        return await func(update, context, auth_ctx, *args, **kwargs)

    return wrapper


def require_admin(func: Callable) -> Callable:
    """
    Requires admin role.

    Must be used after @require_authorized.
    """
    @wraps(func)
    async def wrapper(update, context, auth_ctx: AuthContext, *args, **kwargs):
        if not auth_ctx.is_admin:
            await update.message.reply_text(
                "⛔ Только администраторы могут выполнять это действие."
            )
            logger.warning(
                "Admin permission denied",
                user_id=auth_ctx.tg_user_id,
                workspace_id=auth_ctx.workspace_id,
            )
            return

        return await func(update, context, auth_ctx, *args, **kwargs)

    return wrapper


# Legacy alias for backward compatibility
require_workspace = require_authorized
