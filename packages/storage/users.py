"""AGPARS Bot User Storage Module — CRUD for bot.users."""

from datetime import datetime

from sqlalchemy import delete, select, update

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session, get_session
from packages.storage.models import BotUser, UserRole

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CREATE
# ═══════════════════════════════════════════════════════════════════════════════


def create_user(
    tg_user_id: int,
    tg_username: str | None = None,
    full_name: str | None = None,
    role: UserRole = UserRole.UNAUTHORIZED,
) -> int:
    with get_session() as session:
        user = BotUser(
            tg_user_id=tg_user_id,
            tg_username=tg_username,
            full_name=full_name,
            role=role,
        )
        session.add(user)
        session.flush()
        user_id = user.id
        logger.info("User created", user_id=user_id, tg_user_id=tg_user_id, role=role.value)
        return user_id


# ═══════════════════════════════════════════════════════════════════════════════
# READ
# ═══════════════════════════════════════════════════════════════════════════════


def get_user_by_tg_id(tg_user_id: int) -> dict | None:
    with get_readonly_session() as session:
        query = select(BotUser).where(BotUser.tg_user_id == tg_user_id)
        result = session.execute(query)
        user = result.scalar_one_or_none()
        return _user_to_dict(user) if user else None


def get_user_by_id(user_id: int) -> dict | None:
    with get_readonly_session() as session:
        user = session.get(BotUser, user_id)
        return _user_to_dict(user) if user else None


def list_all_users() -> list[dict]:
    with get_readonly_session() as session:
        query = (
            select(BotUser)
            .where(BotUser.is_active == True)  # noqa: E712
            .order_by(BotUser.created_at.desc())
        )
        result = session.execute(query)
        return [_user_to_dict(u) for u in result.scalars().all()]


def list_users_by_role(role: UserRole) -> list[dict]:
    with get_readonly_session() as session:
        query = (
            select(BotUser)
            .where(BotUser.role == role)
            .where(BotUser.is_active == True)  # noqa: E712
            .order_by(BotUser.created_at.desc())
        )
        result = session.execute(query)
        return [_user_to_dict(u) for u in result.scalars().all()]


# ═══════════════════════════════════════════════════════════════════════════════
# UPDATE
# ═══════════════════════════════════════════════════════════════════════════════


def set_user_role(user_id: int, role: UserRole) -> bool:
    with get_session() as session:
        stmt = (
            update(BotUser)
            .where(BotUser.id == user_id)
            .values(role=role, updated_at=datetime.utcnow())
        )
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info("User role updated", user_id=user_id, role=role.value)
            return True
        return False


def update_user_info(
    tg_user_id: int,
    tg_username: str | None = None,
    full_name: str | None = None,
) -> bool:
    updates: dict = {"updated_at": datetime.utcnow()}
    if tg_username is not None:
        updates["tg_username"] = tg_username
    if full_name is not None:
        updates["full_name"] = full_name
    with get_session() as session:
        stmt = update(BotUser).where(BotUser.tg_user_id == tg_user_id).values(**updates)
        result = session.execute(stmt)
        return result.rowcount > 0


# ═══════════════════════════════════════════════════════════════════════════════
# DELETE
# ═══════════════════════════════════════════════════════════════════════════════


def delete_user(user_id: int) -> bool:
    with get_session() as session:
        stmt = delete(BotUser).where(BotUser.id == user_id)
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info("User deleted (cascade)", user_id=user_id)
            return True
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def get_or_create_user(
    tg_user_id: int,
    tg_username: str | None = None,
    full_name: str | None = None,
    default_role: UserRole = UserRole.UNAUTHORIZED,
) -> dict:
    existing = get_user_by_tg_id(tg_user_id)
    if existing:
        needs_update = False
        if tg_username is not None and existing.get("tg_username") != tg_username:
            needs_update = True
        if full_name is not None and existing.get("full_name") != full_name:
            needs_update = True
        if needs_update:
            update_user_info(tg_user_id, tg_username=tg_username, full_name=full_name)
            existing = get_user_by_tg_id(tg_user_id)
        return existing
    user_id = create_user(
        tg_user_id=tg_user_id,
        tg_username=tg_username,
        full_name=full_name,
        role=default_role,
    )
    return get_user_by_id(user_id)


def _user_to_dict(user: BotUser) -> dict:
    return {
        "id": user.id,
        "tg_user_id": user.tg_user_id,
        "tg_username": user.tg_username,
        "full_name": user.full_name,
        "role": user.role.value,
        "is_active": user.is_active,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
    }
