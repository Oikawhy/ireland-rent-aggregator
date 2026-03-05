"""
AGPARS Outbox Worker

Processes pending events from ops.event_outbox and delivers via Telegram.

Covers T060.
"""

import asyncio
from datetime import datetime

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.observability.metrics import (
    OUTBOX_PENDING,
    OUTBOX_DELIVERY_ATTEMPTS_TOTAL,
)
from packages.storage.db import get_session
from packages.storage.models import EventStatus
from packages.storage.workspaces import get_workspace_by_id
from packages.telegram.render import render_card
from services.notifier.deliver import send_notification
from services.notifier.rate_limit import RateLimiter

logger = get_logger(__name__)


class OutboxWorker:
    """
    Worker that polls ops.event_outbox and delivers notifications.

    Lifecycle per event:
        PENDING → DELIVERING → DELIVERED
                             ↘ FAILED (retry_count < max) → PENDING
                             ↘ DEAD   (retry_count >= max)
    """

    MAX_RETRIES = 5
    DEFAULT_BATCH_SIZE = 50
    DEFAULT_POLL_INTERVAL = 5  # seconds

    def __init__(
        self,
        batch_size: int = DEFAULT_BATCH_SIZE,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
    ):
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.rate_limiter = RateLimiter()
        self._running = False

    async def run(self) -> None:
        """Main worker loop — polls and processes until stopped."""
        self._running = True
        logger.info("Outbox worker started", batch_size=self.batch_size)

        while self._running:
            processed = await self.process_batch()
            if processed == 0:
                await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        """Signal the worker to stop."""
        self._running = False
        logger.info("Outbox worker stopping")

    async def process_batch(self) -> int:
        """
        Process one batch of pending events.

        Returns:
            Number of events processed
        """
        events = self._fetch_pending_events()

        # Update outbox_pending gauge (count remaining pending)
        try:
            with get_session() as session:
                pending_count = session.execute(
                    text("SELECT COUNT(*) FROM ops.event_outbox WHERE status = :status"),
                    {"status": EventStatus.PENDING.value},
                ).scalar() or 0
            OUTBOX_PENDING.set(pending_count)
        except Exception:
            pass

        if not events:
            return 0

        processed = 0
        for event in events:
            try:
                success = await self._process_event(event)
                if success:
                    self._mark_delivered(event["id"])
                    OUTBOX_DELIVERY_ATTEMPTS_TOTAL.labels(result="success").inc()
                else:
                    self._mark_failed(event["id"], event.get("retry_count", 0))
                    OUTBOX_DELIVERY_ATTEMPTS_TOTAL.labels(result="failure").inc()
                processed += 1
            except Exception as e:
                logger.error("Event processing error", event_id=event["id"], error=str(e))
                self._mark_failed(event["id"], event.get("retry_count", 0))
                OUTBOX_DELIVERY_ATTEMPTS_TOTAL.labels(result="failure").inc()
                processed += 1

        logger.info("Batch processed", count=processed)
        return processed

    def _fetch_pending_events(self) -> list[dict]:
        """Fetch and lock a batch of pending events."""
        with get_session() as session:
            # SELECT … FOR UPDATE SKIP LOCKED for concurrency safety
            # Also skip events for digest-mode workspaces (belt-and-suspenders)
            result = session.execute(
                text("""
                    UPDATE ops.event_outbox
                    SET status = :delivering, processed_at = NOW()
                    WHERE id IN (
                        SELECT eo.id FROM ops.event_outbox eo
                        WHERE eo.status = :pending
                        AND eo.workspace_id NOT IN (
                            SELECT s.workspace_id FROM bot.subscriptions s
                            WHERE s.delivery_mode IN ('digest', 'paused') AND s.is_enabled = true
                        )
                        ORDER BY eo.created_at ASC
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, workspace_id, event_type, listing_raw_id,
                              payload, retry_count, created_at
                """),
                {
                    "delivering": EventStatus.DELIVERING.value,
                    "pending": EventStatus.PENDING.value,
                    "limit": self.batch_size,
                },
            )
            rows = result.fetchall()
            session.commit()

            return [dict(row._mapping) for row in rows]

    async def _process_event(self, event: dict) -> bool:
        """Process a single event: render message → rate limit → send."""
        workspace_id = event["workspace_id"]

        # Get workspace to find chat_id
        workspace = get_workspace_by_id(workspace_id)
        if not workspace:
            logger.warning("Workspace not found", workspace_id=workspace_id)
            return False

        if not workspace.get("is_active"):
            logger.debug("Workspace inactive, skipping", workspace_id=workspace_id)
            return True  # Mark as delivered to avoid retrying

        chat_id = workspace["tg_chat_id"]

        # Render message
        payload = event.get("payload", {})
        event_type = event.get("event_type", "new")
        message_text = render_card(payload, event_type)

        # Rate limit check
        if not self.rate_limiter.wait_for_slot(chat_id, max_wait=10.0):
            logger.warning("Rate limit exceeded, deferring", chat_id=chat_id)
            return False

        # Record send in rate limiter
        self.rate_limiter.record_send(chat_id)

        # Send via Telegram
        return await send_notification(
            workspace_id=workspace_id,
            chat_id=chat_id,
            event_id=event["id"],
            message_text=message_text,
        )

    def _mark_delivered(self, event_id: int) -> None:
        """Mark event as delivered."""
        with get_session() as session:
            session.execute(
                text("""
                    UPDATE ops.event_outbox
                    SET status = :status, processed_at = NOW()
                    WHERE id = :event_id
                """),
                {"event_id": event_id, "status": EventStatus.DELIVERED.value},
            )
            session.commit()

    def _mark_failed(self, event_id: int, retry_count: int) -> None:
        """Mark event as failed; move to DEAD if retries exhausted."""
        new_status = EventStatus.FAILED if retry_count < self.MAX_RETRIES else EventStatus.DEAD
        new_retry = retry_count + 1

        with get_session() as session:
            session.execute(
                text("""
                    UPDATE ops.event_outbox
                    SET status = :status, retry_count = :retry_count, processed_at = NOW()
                    WHERE id = :event_id
                """),
                {
                    "event_id": event_id,
                    "status": new_status.value,
                    "retry_count": new_retry,
                },
            )
            session.commit()

        if new_status == EventStatus.DEAD:
            logger.warning("Event moved to DEAD", event_id=event_id, retries=new_retry)
        else:
            logger.info("Event failed, will retry", event_id=event_id, retry=new_retry)
