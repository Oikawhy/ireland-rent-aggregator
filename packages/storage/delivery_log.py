"""
AGPARS Delivery Log Storage

CRUD for Telegram delivery logs in ops.delivery_log.
"""


from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_session

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CREATE
# ═══════════════════════════════════════════════════════════════════════════════


def record_delivery(
    workspace_id: int,
    event_id: int,
    telegram_message_id: int | None = None,
) -> int:
    """
    Record a delivery in the log (for idempotency).

    Args:
        workspace_id: Workspace ID
        event_id: Event outbox ID
        telegram_message_id: Telegram message ID if sent

    Returns:
        Delivery log ID
    """
    with get_session() as session:
        result = session.execute(
            text("""
                INSERT INTO ops.delivery_log (
                    workspace_id, event_id, telegram_message_id, sent_at
                )
                VALUES (:workspace_id, :event_id, :telegram_message_id, NOW())
                ON CONFLICT (workspace_id, event_id) DO UPDATE
                SET telegram_message_id = EXCLUDED.telegram_message_id
                RETURNING id
            """),
            {
                "workspace_id": workspace_id,
                "event_id": event_id,
                "telegram_message_id": telegram_message_id,
            },
        )
        delivery_id = result.scalar_one()
        session.commit()
        return delivery_id


# ═══════════════════════════════════════════════════════════════════════════════
# READ
# ═══════════════════════════════════════════════════════════════════════════════


def was_delivered(workspace_id: int, event_id: int) -> bool:
    """
    Check if an event was already delivered to a workspace.

    Used for idempotency in notification delivery.
    """
    with get_session() as session:
        result = session.execute(
            text("""
                SELECT 1 FROM ops.delivery_log
                WHERE workspace_id = :workspace_id AND event_id = :event_id
                LIMIT 1
            """),
            {"workspace_id": workspace_id, "event_id": event_id},
        )
        return result.fetchone() is not None


def get_deliveries_for_workspace(
    workspace_id: int,
    limit: int = 50,
) -> list[dict]:
    """Get recent deliveries for a workspace."""
    with get_session() as session:
        result = session.execute(
            text("""
                SELECT * FROM ops.delivery_log
                WHERE workspace_id = :workspace_id
                ORDER BY sent_at DESC
                LIMIT :limit
            """),
            {"workspace_id": workspace_id, "limit": limit},
        )
        return [dict(row._mapping) for row in result]


def get_delivery_stats(since_hours: int = 24) -> dict:
    """Get delivery statistics."""
    with get_session() as session:
        result = session.execute(
            text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT workspace_id) as unique_workspaces
                FROM ops.delivery_log
                WHERE sent_at > NOW() - INTERVAL '1 hour' * :hours
            """),
            {"hours": since_hours},
        )
        row = result.fetchone()
        if row:
            return {
                "total_deliveries": row.total,
                "unique_workspaces": row.unique_workspaces,
            }
        return {"total_deliveries": 0, "unique_workspaces": 0}
