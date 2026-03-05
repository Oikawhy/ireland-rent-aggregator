"""
AGPARS Subscription Storage Module

CRUD operations for workspace subscriptions (filter configurations).
"""

from datetime import datetime

from sqlalchemy import delete, select, text, update

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session, get_session
from packages.storage.models import DeliveryMode, Subscription

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CREATE
# ═══════════════════════════════════════════════════════════════════════════════


def create_subscription(
    workspace_id: int,
    filters: dict | None = None,
    delivery_mode: DeliveryMode = DeliveryMode.INSTANT,
    name: str | None = None,
    digest_schedule: dict | None = None,
) -> int:
    """
    Create a new subscription for a workspace.

    Args:
        workspace_id: Workspace ID
        filters: Filter configuration (cities, budget, beds, etc.)
        delivery_mode: INSTANT, DIGEST, or PAUSED
        name: Optional subscription name
        digest_schedule: Schedule for digest mode

    Returns:
        Subscription ID
    """
    with get_session() as session:
        subscription = Subscription(
            workspace_id=workspace_id,
            name=name,
            filters=filters or {},
            delivery_mode=delivery_mode,
            digest_schedule=digest_schedule,
            is_enabled=True,
        )
        session.add(subscription)
        session.flush()
        subscription_id = subscription.id
        logger.info(
            "Subscription created",
            subscription_id=subscription_id,
            workspace_id=workspace_id,
            delivery_mode=delivery_mode.value,
        )
        return subscription_id


# ═══════════════════════════════════════════════════════════════════════════════
# READ
# ═══════════════════════════════════════════════════════════════════════════════


def get_subscription(subscription_id: int) -> dict | None:
    """Get subscription by ID."""
    with get_readonly_session() as session:
        subscription = session.get(Subscription, subscription_id)
        if subscription:
            return _subscription_to_dict(subscription)
        return None


def get_subscriptions_for_workspace(workspace_id: int, enabled_only: bool = False) -> list[dict]:
    """Get all subscriptions for a workspace."""
    with get_readonly_session() as session:
        query = select(Subscription).where(Subscription.workspace_id == workspace_id)
        if enabled_only:
            query = query.where(Subscription.is_enabled == True)  # noqa: E712
        result = session.execute(query)
        return [_subscription_to_dict(s) for s in result.scalars().all()]


def get_active_subscriptions() -> list[dict]:
    """
    Get all active subscriptions across all workspaces.

    Used by scheduler for job creation.
    """
    with get_readonly_session() as session:
        query = select(Subscription).where(Subscription.is_enabled == True)  # noqa: E712
        result = session.execute(query)
        return [_subscription_to_dict(s) for s in result.scalars().all()]


def get_subscriptions_by_city(city_id: int) -> list[dict]:
    """Get all subscriptions that include a specific city."""
    subscriptions = get_active_subscriptions()
    return [
        sub for sub in subscriptions
        if city_id in sub["filters"].get("city_ids", [])
    ]


def get_subscriptions_by_county(county: str) -> list[dict]:
    """Get all subscriptions that include a specific county."""
    subscriptions = get_active_subscriptions()
    county_lower = county.lower()
    return [
        sub for sub in subscriptions
        if county_lower in [c.lower() for c in sub["filters"].get("counties", [])]
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# UPDATE
# ═══════════════════════════════════════════════════════════════════════════════


def update_subscription(
    subscription_id: int,
    name: str | None = None,
    filters: dict | None = None,
    delivery_mode: DeliveryMode | None = None,
    digest_schedule: dict | None = None,
    is_enabled: bool | None = None,
) -> bool:
    """
    Update subscription fields.

    Returns:
        True if update was successful
    """
    updates = {}
    if name is not None:
        updates["name"] = name
    if filters is not None:
        updates["filters"] = filters
    if delivery_mode is not None:
        updates["delivery_mode"] = delivery_mode
    if digest_schedule is not None:
        updates["digest_schedule"] = digest_schedule
    if is_enabled is not None:
        updates["is_enabled"] = is_enabled

    if not updates:
        return False

    updates["updated_at"] = datetime.utcnow()

    with get_session() as session:
        stmt = update(Subscription).where(Subscription.id == subscription_id).values(**updates)
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info(
                "Subscription updated",
                subscription_id=subscription_id,
                updates=list(updates.keys()),
            )
            return True
        return False


def enable_subscription(subscription_id: int) -> bool:
    """Enable a subscription."""
    return update_subscription(subscription_id, is_enabled=True)


def disable_subscription(subscription_id: int) -> bool:
    """Disable a subscription."""
    return update_subscription(subscription_id, is_enabled=False)


def set_delivery_mode(subscription_id: int, mode: DeliveryMode) -> bool:
    """Set subscription delivery mode."""
    return update_subscription(subscription_id, delivery_mode=mode)


# ═══════════════════════════════════════════════════════════════════════════════
# DELETE
# ═══════════════════════════════════════════════════════════════════════════════


def delete_subscription(subscription_id: int) -> bool:
    """Delete a subscription."""
    with get_session() as session:
        stmt = delete(Subscription).where(Subscription.id == subscription_id)
        result = session.execute(stmt)
        if result.rowcount > 0:
            logger.info("Subscription deleted", subscription_id=subscription_id)
            return True
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _subscription_to_dict(subscription: Subscription) -> dict:
    """Convert Subscription ORM object to dictionary."""
    return {
        "id": subscription.id,
        "workspace_id": subscription.workspace_id,
        "name": subscription.name,
        "filters": subscription.filters or {},
        "delivery_mode": subscription.delivery_mode.value,
        "digest_schedule": subscription.digest_schedule,
        "is_enabled": subscription.is_enabled,
        "created_at": subscription.created_at,
        "updated_at": subscription.updated_at,
    }


def ensure_default_subscription() -> bool:
    """
    Ensure at least one active subscription exists.

    If the database has no enabled subscriptions, creates a system workspace
    and a global subscription (filters={} → all sources, all cities).

    This allows the scheduler to bootstrap itself on first startup
    without manual intervention.

    Returns:
        True if a default subscription was created, False if one already existed.
    """
    # Quick check: any active subscriptions?
    existing = get_active_subscriptions()
    if existing:
        return False

    logger.warning(
        "No active subscriptions found — auto-seeding default subscription"
    )

    # System workspace with a dummy chat_id
    SYSTEM_CHAT_ID = 999999

    with get_session() as session:
        # 1. Ensure system workspace exists
        session.execute(text("""
            INSERT INTO bot.workspaces (type, tg_chat_id, title, is_active)
            VALUES ('personal', :chat_id, 'System Workspace', true)
            ON CONFLICT (tg_chat_id) DO NOTHING
        """), {"chat_id": SYSTEM_CHAT_ID})

        # 2. Get workspace ID (may have been created on a previous run)
        ws_id = session.execute(
            text("SELECT id FROM bot.workspaces WHERE tg_chat_id = :chat_id"),
            {"chat_id": SYSTEM_CHAT_ID},
        ).scalar()

        if ws_id is None:
            logger.error("Failed to create or find system workspace")
            return False

        # 3. Create global subscription (filters={} = all sources, all cities)
        session.execute(text("""
            INSERT INTO bot.subscriptions
                (workspace_id, name, filters, delivery_mode, is_enabled)
            VALUES
                (:ws_id, 'Global Scrape', '{}', 'instant', true)
            ON CONFLICT DO NOTHING
        """), {"ws_id": ws_id})

        session.commit()

    logger.info(
        "Default subscription auto-seeded",
        workspace_id=ws_id,
        name="Global Scrape",
    )
    return True


def get_active_city_ids() -> set[int]:
    """Get set of all city IDs from active subscriptions."""
    subscriptions = get_active_subscriptions()
    city_ids = set()
    for sub in subscriptions:
        city_ids.update(sub["filters"].get("city_ids", []))
    return city_ids

