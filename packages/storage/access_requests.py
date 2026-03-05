"""AGPARS Access Request Storage — CRUD for bot.access_requests."""

from datetime import datetime

from sqlalchemy import select, update

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session, get_session
from packages.storage.models import AccessRequest, AccessRequestStatus

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CREATE
# ═══════════════════════════════════════════════════════════════════════════════


def create_access_request(user_id: int) -> int:
    with get_session() as session:
        req = AccessRequest(user_id=user_id, status=AccessRequestStatus.PENDING)
        session.add(req)
        session.flush()
        req_id = req.id
        logger.info("Access request created", request_id=req_id, user_id=user_id)
        return req_id


# ═══════════════════════════════════════════════════════════════════════════════
# READ
# ═══════════════════════════════════════════════════════════════════════════════


def get_pending_request_for_user(user_id: int) -> dict | None:
    with get_readonly_session() as session:
        query = (
            select(AccessRequest)
            .where(AccessRequest.user_id == user_id)
            .where(AccessRequest.status == AccessRequestStatus.PENDING)
            .order_by(AccessRequest.created_at.desc())
            .limit(1)
        )
        result = session.execute(query)
        req = result.scalar_one_or_none()
        return _request_to_dict(req) if req else None


def get_request_by_id(request_id: int) -> dict | None:
    with get_readonly_session() as session:
        req = session.get(AccessRequest, request_id)
        return _request_to_dict(req) if req else None


# ═══════════════════════════════════════════════════════════════════════════════
# UPDATE
# ═══════════════════════════════════════════════════════════════════════════════


def approve_request(request_id: int, reviewed_by_user_id: int | None = None) -> bool:
    return _update_request_status(request_id, AccessRequestStatus.APPROVED, reviewed_by_user_id)


def decline_request(request_id: int, reviewed_by_user_id: int | None = None) -> bool:
    return _update_request_status(request_id, AccessRequestStatus.DECLINED, reviewed_by_user_id)


def set_admin_message_id(request_id: int, message_id: int) -> bool:
    with get_session() as session:
        stmt = (
            update(AccessRequest)
            .where(AccessRequest.id == request_id)
            .values(admin_message_id=message_id)
        )
        result = session.execute(stmt)
        return result.rowcount > 0


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _update_request_status(
    request_id: int,
    status: AccessRequestStatus,
    reviewed_by_user_id: int | None = None,
) -> bool:
    with get_session() as session:
        stmt = (
            update(AccessRequest)
            .where(AccessRequest.id == request_id)
            .values(
                status=status,
                reviewed_by_user_id=reviewed_by_user_id,
                reviewed_at=datetime.utcnow(),
            )
        )
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info(
                "Access request updated",
                request_id=request_id,
                status=status.value,
                reviewed_by=reviewed_by_user_id,
            )
            return True
        return False


def _request_to_dict(req: AccessRequest) -> dict:
    return {
        "id": req.id,
        "user_id": req.user_id,
        "status": req.status.value,
        "admin_message_id": req.admin_message_id,
        "reviewed_by_user_id": req.reviewed_by_user_id,
        "created_at": req.created_at,
        "reviewed_at": req.reviewed_at,
    }
