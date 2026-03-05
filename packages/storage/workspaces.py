"""
AGPARS Workspace Storage Module

CRUD operations for workspaces (Telegram groups/personal chats).
"""

from datetime import datetime

from sqlalchemy import select, update

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session, get_session
from packages.storage.models import Workspace, WorkspaceType

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CREATE
# ═══════════════════════════════════════════════════════════════════════════════


def create_workspace(
    workspace_type: WorkspaceType,
    tg_chat_id: int,
    title: str | None = None,
    timezone: str = "Europe/Dublin",
    settings: dict | None = None,
    owner_user_id: int | None = None,
) -> int:
    """
    Create a new workspace.

    Args:
        workspace_type: PERSONAL or GROUP
        tg_chat_id: Telegram chat ID
        title: Chat title (optional for personal)
        timezone: Timezone for digest scheduling
        settings: Additional settings (JSONB)
        owner_user_id: Owner bot user ID

    Returns:
        Workspace ID
    """
    with get_session() as session:
        workspace = Workspace(
            type=workspace_type,
            tg_chat_id=tg_chat_id,
            title=title,
            timezone=timezone,
            settings=settings or {},
            is_active=True,
            owner_user_id=owner_user_id,
        )
        session.add(workspace)
        session.flush()
        workspace_id = workspace.id
        logger.info(
            "Workspace created",
            workspace_id=workspace_id,
            type=workspace_type.value,
            tg_chat_id=tg_chat_id,
        )
        return workspace_id


# ═══════════════════════════════════════════════════════════════════════════════
# READ
# ═══════════════════════════════════════════════════════════════════════════════


def get_workspace_by_id(workspace_id: int) -> dict | None:
    """Get workspace by ID."""
    with get_readonly_session() as session:
        workspace = session.get(Workspace, workspace_id)
        if workspace:
            return _workspace_to_dict(workspace)
        return None


def get_workspace_by_chat_id(tg_chat_id: int) -> dict | None:
    """Get workspace by Telegram chat ID."""
    with get_readonly_session() as session:
        query = select(Workspace).where(Workspace.tg_chat_id == tg_chat_id)
        result = session.execute(query)
        workspace = result.scalar_one_or_none()
        if workspace:
            return _workspace_to_dict(workspace)
        return None


def list_active_workspaces() -> list[dict]:
    """Get all active workspaces."""
    with get_readonly_session() as session:
        query = select(Workspace).where(Workspace.is_active == True)  # noqa: E712
        result = session.execute(query)
        return [_workspace_to_dict(w) for w in result.scalars().all()]


def get_workspace_count() -> int:
    """Get count of active workspaces."""
    return len(list_active_workspaces())


# ═══════════════════════════════════════════════════════════════════════════════
# UPDATE
# ═══════════════════════════════════════════════════════════════════════════════


def update_workspace(
    workspace_id: int,
    title: str | None = None,
    timezone: str | None = None,
    settings: dict | None = None,
) -> bool:
    """
    Update workspace fields.

    Returns:
        True if update was successful
    """
    updates = {}
    if title is not None:
        updates["title"] = title
    if timezone is not None:
        updates["timezone"] = timezone
    if settings is not None:
        updates["settings"] = settings

    if not updates:
        return False

    updates["updated_at"] = datetime.utcnow()

    with get_session() as session:
        stmt = update(Workspace).where(Workspace.id == workspace_id).values(**updates)
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info("Workspace updated", workspace_id=workspace_id, updates=list(updates.keys()))
            return True
        return False


def deactivate_workspace(workspace_id: int) -> bool:
    """Deactivate a workspace (soft delete)."""
    with get_session() as session:
        stmt = (
            update(Workspace)
            .where(Workspace.id == workspace_id)
            .values(is_active=False, updated_at=datetime.utcnow())
        )
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info("Workspace deactivated", workspace_id=workspace_id)
            return True
        return False


def activate_workspace(workspace_id: int) -> bool:
    """Reactivate a workspace."""
    with get_session() as session:
        stmt = (
            update(Workspace)
            .where(Workspace.id == workspace_id)
            .values(is_active=True, updated_at=datetime.utcnow())
        )
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info("Workspace activated", workspace_id=workspace_id)
            return True
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _workspace_to_dict(workspace: Workspace) -> dict:
    """Convert Workspace ORM object to dictionary."""
    return {
        "id": workspace.id,
        "type": workspace.type.value,
        "tg_chat_id": workspace.tg_chat_id,
        "title": workspace.title,
        "timezone": workspace.timezone,
        "settings": workspace.settings or {},
        "is_active": workspace.is_active,
        "owner_user_id": workspace.owner_user_id,
        "created_at": workspace.created_at,
        "updated_at": workspace.updated_at,
    }


def get_or_create_workspace(
    workspace_type: WorkspaceType,
    tg_chat_id: int,
    title: str | None = None,
    owner_user_id: int | None = None,
) -> dict:
    """Get existing workspace or create a new one."""
    existing = get_workspace_by_chat_id(tg_chat_id)
    if existing:
        return existing

    workspace_id = create_workspace(
        workspace_type=workspace_type,
        tg_chat_id=tg_chat_id,
        title=title,
        owner_user_id=owner_user_id,
    )
    return get_workspace_by_id(workspace_id)


def delete_workspace_by_chat_id(tg_chat_id: int) -> bool:
    """Delete workspace by Telegram chat ID (cascade)."""
    from sqlalchemy import delete as sa_delete
    with get_session() as session:
        stmt = sa_delete(Workspace).where(Workspace.tg_chat_id == tg_chat_id)
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info("Workspace deleted", tg_chat_id=tg_chat_id)
            return True
        return False
