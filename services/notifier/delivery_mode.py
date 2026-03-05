"""
AGPARS Delivery Mode Manager

Handles delivery mode transitions (instant/digest/paused).

Covers T071.
"""

from packages.observability.logger import get_logger
from packages.storage.models import DeliveryMode
from packages.storage.subscriptions import (
    get_subscription,
    set_delivery_mode,
    update_subscription,
)

logger = get_logger(__name__)


def switch_to_instant(subscription_id: int) -> bool:
    """
    Switch subscription to instant delivery mode.

    Returns:
        True if switch was successful
    """
    return _switch_mode(subscription_id, DeliveryMode.INSTANT)


def switch_to_digest(
    subscription_id: int,
    schedule: dict | None = None,
) -> bool:
    """
    Switch subscription to digest delivery mode.

    Args:
        subscription_id: Subscription to switch
        schedule: Digest schedule config (frequency, time, etc.)

    Returns:
        True if switch was successful
    """
    default_schedule = {
        "frequency": "daily",
        "time": "09:00",
        "timezone": "Europe/Dublin",
    }

    actual_schedule = schedule or default_schedule

    update_subscription(
        subscription_id,
        delivery_mode=DeliveryMode.DIGEST,
        digest_schedule=actual_schedule,
    )

    logger.info(
        "Switched to digest mode",
        subscription_id=subscription_id,
        schedule=actual_schedule,
    )
    return True


def switch_to_paused(subscription_id: int) -> bool:
    """
    Switch subscription to paused delivery mode.

    Returns:
        True if switch was successful
    """
    return _switch_mode(subscription_id, DeliveryMode.PAUSED)


def get_delivery_mode(subscription_id: int) -> str:
    """
    Get current delivery mode for subscription.

    Returns:
        Delivery mode string
    """
    sub = get_subscription(subscription_id)
    if not sub:
        return "unknown"
    return sub.get("delivery_mode", "instant")


def _switch_mode(subscription_id: int, mode: DeliveryMode) -> bool:
    """Switch subscription delivery mode."""
    try:
        set_delivery_mode(subscription_id, mode)
        logger.info("Delivery mode changed", subscription_id=subscription_id, mode=mode.value)
        return True
    except Exception as e:
        logger.error("Failed to switch mode", subscription_id=subscription_id, error=str(e))
        return False
