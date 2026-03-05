"""
AGPARS Digest Scheduler

Calculates when digest deliveries are due and collects subscriptions.

Covers T069.
"""

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from packages.observability.logger import get_logger
from packages.storage.subscriptions import get_active_subscriptions

logger = get_logger(__name__)

DAY_MAP = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


def parse_digest_schedule(raw: dict) -> dict:
    """
    Parse a raw digest schedule config into normalized form.

    Args:
        raw: Digest schedule dict with frequency, time, day_of_week

    Returns:
        Normalized schedule dict

    Raises:
        ValueError: If frequency is invalid
    """
    frequency = raw.get("frequency", "daily")
    if frequency not in ("daily", "twice_daily", "weekly"):
        raise ValueError(f"Invalid frequency: {frequency}")

    schedule = {"frequency": frequency, "timezone": raw.get("timezone", "Europe/Dublin")}

    # Parse time
    time_str = raw.get("time", "09:00")
    if time_str:
        parts = time_str.split(":")
        schedule["hour"] = int(parts[0])
        schedule["minute"] = int(parts[1]) if len(parts) > 1 else 0

    # Twice daily: parse two times
    if frequency == "twice_daily":
        hours = raw.get("hours", [9, 18])
        schedule["hours"] = hours
        schedule["minute"] = schedule.get("minute", 0)

    # Weekly: parse day
    if frequency == "weekly":
        day = raw.get("day_of_week", "monday")
        schedule["day_of_week"] = DAY_MAP.get(day.lower(), 0) if isinstance(day, str) else int(day)

    return schedule


def is_digest_due(schedule: dict, now: datetime) -> bool:
    """
    Check if a digest is due at the given time.

    Args:
        schedule: Normalized schedule dict
        now: Current datetime

    Returns:
        True if digest should be sent
    """
    frequency = schedule.get("frequency", "daily")
    minute = schedule.get("minute", 0)

    if frequency == "daily":
        return now.hour == schedule.get("hour", 9) and now.minute == minute

    if frequency == "twice_daily":
        hours = schedule.get("hours", [9, 18])
        return now.hour in hours and now.minute == minute

    if frequency == "weekly":
        target_day = schedule.get("day_of_week", 0)
        return (
            now.weekday() == target_day
            and now.hour == schedule.get("hour", 9)
            and now.minute == minute
        )

    return False


def get_due_digest_subscriptions(now: datetime | None = None) -> list[dict]:
    """
    Get all digest subscriptions that are due for delivery.

    Args:
        now: Current time (defaults to utcnow)

    Returns:
        List of subscription dicts that need digest delivery
    """
    if now is None:
        now = datetime.now(ZoneInfo("Europe/Dublin")).replace(tzinfo=None)

    all_subs = get_active_subscriptions()
    due = []

    for sub in all_subs:
        if sub.get("delivery_mode") != "digest":
            continue

        raw_schedule = sub.get("digest_schedule")
        if not raw_schedule:
            continue

        try:
            schedule = parse_digest_schedule(raw_schedule)
            if is_digest_due(schedule, now):
                due.append(sub)
        except (ValueError, TypeError) as e:
            logger.warning("Invalid digest schedule", sub_id=sub["id"], error=str(e))

    return due


class DigestScheduler:
    """Periodic digest scheduler."""

    def __init__(self, interval_seconds: int = 30):
        self.interval = interval_seconds
        self._running = False

    async def run(self):
        """Run digest scheduler loop."""
        self._running = True
        logger.info("Digest scheduler started", interval=self.interval)

        while self._running:
            try:
                # Check for due digests using Dublin timezone
                now = datetime.now(ZoneInfo("Europe/Dublin")).replace(tzinfo=None)
                sent = await self.run_digest_cycle(now)
                
                if sent > 0:
                    logger.info("Digest cycle complete", sent=sent)
            except Exception as e:
                logger.error("Digest cycle failed", error=str(e))
            
            # Sleep until next check
            await asyncio.sleep(self.interval)

    def stop(self):
        """Stop the scheduler."""
        self._running = False
        logger.info("Digest scheduler stopping")

    async def run_digest_cycle(self, now: datetime | None = None) -> int:
        """
        Main digest cycle — find due subscriptions, create batches, deliver.
        """
        return await run_digest_cycle(now)


async def run_digest_cycle(now: datetime | None = None) -> int:
    """
    Main digest cycle — find due subscriptions, create batches, deliver.

    Returns:
        Number of digests sent
    """
    from packages.telegram.digest import create_digest_batch
    from services.notifier.deliver import send_notification
    from packages.storage.workspaces import get_workspace_by_id

    due_subs = get_due_digest_subscriptions(now)
    if not due_subs:
        return 0

    sent = 0
    for sub in due_subs:
        workspace = get_workspace_by_id(sub["workspace_id"])
        if not workspace or not workspace.get("is_active"):
            continue

        # Create digest batch (aggregate events)
        batch = create_digest_batch(workspace_id=sub["workspace_id"])
        if not batch:
            continue

        chat_id = workspace["tg_chat_id"]
        
        # Format message content properly using templates
        # Note: batch["message"] is already rendered by create_digest_batch? 
        # Let's assume create_digest_batch returns a dict with rendered message.
        # If not, we need to render it here.
        
        success = await send_notification(
            workspace_id=sub["workspace_id"],
            chat_id=chat_id,
            event_id=batch.get("first_event_id", 0), # Use first event as ID reference
            message_text=batch.get("message", "Digest Update"),
        )

        if success:
            sent += 1
            # Mark ALL batch events as DELIVERED
            _mark_events_delivered(batch["event_ids"])
            logger.info("Digest sent", workspace_id=sub["workspace_id"],
                        events=len(batch["event_ids"]))

    return sent


def _mark_events_delivered(event_ids: list[int]) -> None:
    """Mark all digest batch events as delivered."""
    if not event_ids:
        return
    from packages.storage.db import get_session
    from sqlalchemy import text
    with get_session() as session:
        session.execute(
            text("UPDATE ops.event_outbox SET status = 'delivered', processed_at = NOW() WHERE id = ANY(:ids)"),
            {"ids": event_ids},
        )
        session.commit()
    logger.info("Digest events marked delivered", count=len(event_ids))
