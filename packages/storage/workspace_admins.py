"""
AGPARS Workspace Admin Storage Module

CRUD operations for workspace administrators.
"""

from sqlalchemy import delete, select

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session, get_session
from packages.storage.models import Workspace, WorkspaceAdmin

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CREATE / DELETE
# ═══════════════════════════════════════════════════════════════════════════════


def add_admin(workspace_id: int, tg_user_id: int) -> int | None:
    """
    Add an admin to a workspace.

    Returns:
        Admin record ID or None if already exists
    """
    # Check if already admin
    if is_admin(workspace_id, tg_user_id):
        logger.debug("User already admin", workspace_id=workspace_id, tg_user_id=tg_user_id)
        return None

    with get_session() as session:
        admin = WorkspaceAdmin(
            workspace_id=workspace_id,
            tg_user_id=tg_user_id,
        )
        session.add(admin)
        session.flush()
        admin_id = admin.id
        logger.info(
            "Admin added",
            admin_id=admin_id,
            workspace_id=workspace_id,
            tg_user_id=tg_user_id,
        )
        return admin_id


def remove_admin(workspace_id: int, tg_user_id: int) -> bool:
    """
    Remove an admin from a workspace.

    Returns:
        True if admin was removed
    """
    with get_session() as session:
        stmt = (
            delete(WorkspaceAdmin)
            .where(WorkspaceAdmin.workspace_id == workspace_id)
            .where(WorkspaceAdmin.tg_user_id == tg_user_id)
        )
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info("Admin removed", workspace_id=workspace_id, tg_user_id=tg_user_id)
            return True
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# READ
# ═══════════════════════════════════════════════════════════════════════════════


def is_admin(workspace_id: int, tg_user_id: int) -> bool:
    """Check if a user is an admin of a workspace."""
    with get_readonly_session() as session:
        query = (
            select(WorkspaceAdmin)
            .where(WorkspaceAdmin.workspace_id == workspace_id)
            .where(WorkspaceAdmin.tg_user_id == tg_user_id)
        )
        result = session.execute(query)
        return result.scalar_one_or_none() is not None


def get_admins(workspace_id: int) -> list[dict]:
    """Get all admins for a workspace."""
    with get_readonly_session() as session:
        query = select(WorkspaceAdmin).where(WorkspaceAdmin.workspace_id == workspace_id)
        result = session.execute(query)
        return [
            {
                "id": admin.id,
                "workspace_id": admin.workspace_id,
                "tg_user_id": admin.tg_user_id,
                "added_at": admin.added_at,
            }
            for admin in result.scalars().all()
        ]


def get_admin_user_ids(workspace_id: int) -> list[int]:
    """Get list of admin Telegram user IDs for a workspace."""
    admins = get_admins(workspace_id)
    return [admin["tg_user_id"] for admin in admins]


def get_workspaces_for_user(tg_user_id: int) -> list[dict]:
    """Get all workspaces where user is an admin."""
    with get_readonly_session() as session:
        query = (
            select(Workspace)
            .join(WorkspaceAdmin, Workspace.id == WorkspaceAdmin.workspace_id)
            .where(WorkspaceAdmin.tg_user_id == tg_user_id)
            .where(Workspace.is_active == True)  # noqa: E712
        )
        result = session.execute(query)
        return [
            {
                "id": ws.id,
                "type": ws.type.value,
                "tg_chat_id": ws.tg_chat_id,
                "title": ws.title,
            }
            for ws in result.scalars().all()
        ]


def count_admins(workspace_id: int) -> int:
    """Get count of admins for a workspace."""
    return len(get_admins(workspace_id))


# ═══════════════════════════════════════════════════════════════════════════════
# BULK OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def sync_admins(workspace_id: int, tg_user_ids: list[int]) -> dict:
    """
    Sync workspace admins with a list of Telegram user IDs.

    Adds new admins and removes those not in the list.

    Returns:
        Dict with added/removed counts
    """
    current_ids = set(get_admin_user_ids(workspace_id))
    new_ids = set(tg_user_ids)

    to_add = new_ids - current_ids
    to_remove = current_ids - new_ids

    added = 0
    removed = 0

    for user_id in to_add:
        if add_admin(workspace_id, user_id):
            added += 1

    for user_id in to_remove:
        if remove_admin(workspace_id, user_id):
            removed += 1

    logger.info(
        "Admins synced",
        workspace_id=workspace_id,
        added=added,
        removed=removed,
    )
    return {"added": added, "removed": removed}
