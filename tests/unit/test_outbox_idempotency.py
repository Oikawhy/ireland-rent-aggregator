"""
AGPARS Outbox Idempotency Unit Tests

Tests for T031: delivery idempotency, rate limiting, and worker skip logic.
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ═══════════════════════════════════════════════════════════════════════════════
# DELIVERY IDEMPOTENCY TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestDeliveryIdempotency:
    """Tests for deliver.send_notification idempotency."""

    @pytest.mark.asyncio
    @patch("services.notifier.deliver.was_delivered", return_value=True)
    @patch("services.notifier.deliver._get_sender")
    async def test_skip_already_delivered(self, mock_sender, mock_was_delivered):
        """T031: Duplicate delivery returns early without sending."""
        from services.notifier.deliver import send_notification

        result = await send_notification(
            workspace_id=1,
            chat_id=12345,
            event_id=42,
            message_text="test",
        )

        assert result is True  # Returns True (already done)
        mock_sender.return_value.send_message.assert_not_called()

    @pytest.mark.asyncio
    @patch("services.notifier.deliver.record_delivery")
    @patch("services.notifier.deliver.was_delivered", return_value=False)
    @patch("services.notifier.deliver._get_sender")
    async def test_new_delivery_records_log(self, mock_get_sender, mock_was, mock_record):
        """T031: Successful delivery records in delivery_log."""
        from services.notifier.deliver import send_notification

        sender = AsyncMock()
        sender.send_message.return_value = {"message_id": 999}
        mock_get_sender.return_value = sender

        result = await send_notification(
            workspace_id=1,
            chat_id=12345,
            event_id=42,
            message_text="Hello!",
        )

        assert result is True
        mock_record.assert_called_once_with(
            workspace_id=1,
            event_id=42,
            telegram_message_id=999,
        )

    @pytest.mark.asyncio
    @patch("services.notifier.deliver.was_delivered", return_value=False)
    @patch("services.notifier.deliver._get_sender")
    async def test_failed_delivery_returns_false(self, mock_get_sender, mock_was):
        """T031: Failed send returns False (no log recorded)."""
        from services.notifier.deliver import send_notification

        sender = AsyncMock()
        sender.send_message.return_value = None  # Failure
        mock_get_sender.return_value = sender

        result = await send_notification(
            workspace_id=1,
            chat_id=12345,
            event_id=42,
            message_text="test",
        )

        assert result is False


# ═══════════════════════════════════════════════════════════════════════════════
# WAS_DELIVERED TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestWasDelivered:
    """Tests for delivery_log.was_delivered + record_delivery."""

    @patch("packages.storage.delivery_log.get_readonly_session")
    def test_was_delivered_true(self, mock_session):
        """was_delivered returns True when record exists."""
        from packages.storage.delivery_log import was_delivered

        # Mock a result that has a row
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = MagicMock(return_value=session_ctx)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        session_ctx.execute.return_value.scalar_one_or_none.return_value = 1

        result = was_delivered(workspace_id=1, event_id=42)
        assert result is True

    @patch("packages.storage.delivery_log.get_readonly_session")
    def test_was_delivered_false(self, mock_session):
        """was_delivered returns False when no record."""
        from packages.storage.delivery_log import was_delivered

        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = MagicMock(return_value=session_ctx)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        session_ctx.execute.return_value.scalar_one_or_none.return_value = None

        result = was_delivered(workspace_id=1, event_id=42)
        assert result is False


# ═══════════════════════════════════════════════════════════════════════════════
# RATE LIMITER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestRateLimiter:
    """Tests for rate_limit.RateLimiter."""

    @patch("services.notifier.rate_limit.get_redis_client")
    def test_can_send_under_limit(self, mock_redis):
        """can_send returns True when under limit."""
        from services.notifier.rate_limit import RateLimiter

        client = MagicMock()
        mock_redis.return_value = client

        # Pipeline mock: zremrangebyscore + zcard
        pipe = MagicMock()
        pipe.execute.return_value = [0, 0]  # 0 removed, 0 current count
        client.pipeline.return_value = pipe

        limiter = RateLimiter(global_limit=30, per_chat_limit=1)
        assert limiter.can_send(12345) is True

    @patch("services.notifier.rate_limit.get_redis_client")
    def test_can_send_over_global_limit(self, mock_redis):
        """can_send returns False when global limit exceeded."""
        from services.notifier.rate_limit import RateLimiter

        client = MagicMock()
        mock_redis.return_value = client

        # Pipeline mock: global check shows 30 (at limit)
        pipe = MagicMock()
        pipe.execute.return_value = [0, 30]
        client.pipeline.return_value = pipe

        limiter = RateLimiter(global_limit=30, per_chat_limit=1)
        assert limiter.can_send(12345) is False

    @patch("services.notifier.rate_limit.get_redis_client")
    def test_record_send(self, mock_redis):
        """record_send adds entries to both global and per-chat windows."""
        from services.notifier.rate_limit import RateLimiter

        client = MagicMock()
        mock_redis.return_value = client

        pipe = MagicMock()
        client.pipeline.return_value = pipe

        limiter = RateLimiter()
        limiter.record_send(12345)

        # Should have 2 zadd calls (global + chat) + 2 expire
        assert pipe.zadd.call_count == 2
        assert pipe.expire.call_count == 2
        pipe.execute.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════════
# OUTBOX ENQUEUE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestOutboxEnqueue:
    """Tests for outbox_enqueue.enqueue_listing_event."""

    @patch("services.notifier.outbox_enqueue._insert_event", return_value=100)
    @patch("services.notifier.outbox_enqueue.matches_subscription", return_value=True)
    @patch("services.notifier.outbox_enqueue.get_active_subscriptions")
    def test_enqueue_matching_subscription(self, mock_subs, mock_match, mock_insert):
        """Events created for matching subscriptions."""
        from services.notifier.outbox_enqueue import enqueue_listing_event

        mock_subs.return_value = [
            {"id": 1, "workspace_id": 10, "filters": {}, "delivery_mode": "instant"},
        ]

        ids = enqueue_listing_event(
            raw_id=5,
            event_type="new",
            listing_data={"price": 1500, "city_id": 1},
        )

        assert ids == [100]
        mock_insert.assert_called_once()

    @patch("services.notifier.outbox_enqueue.matches_subscription", return_value=False)
    @patch("services.notifier.outbox_enqueue.get_active_subscriptions")
    def test_no_match_no_event(self, mock_subs, mock_match):
        """No events created when listing doesn't match."""
        from services.notifier.outbox_enqueue import enqueue_listing_event

        mock_subs.return_value = [
            {"id": 1, "workspace_id": 10, "filters": {"max_budget": 1000}, "delivery_mode": "instant"},
        ]

        ids = enqueue_listing_event(
            raw_id=5,
            event_type="new",
            listing_data={"price": 2000},
        )

        assert ids == []

    @patch("services.notifier.outbox_enqueue.get_active_subscriptions")
    def test_paused_subscription_skipped(self, mock_subs):
        """Paused subscriptions don't receive events."""
        from services.notifier.outbox_enqueue import enqueue_listing_event

        mock_subs.return_value = [
            {"id": 1, "workspace_id": 10, "filters": {}, "delivery_mode": "paused"},
        ]

        ids = enqueue_listing_event(
            raw_id=5,
            event_type="new",
            listing_data={"price": 1500},
        )

        assert ids == []

    @patch("services.notifier.outbox_enqueue.get_active_subscriptions")
    def test_invalid_event_type(self, mock_subs):
        """Invalid event type returns empty list."""
        from services.notifier.outbox_enqueue import enqueue_listing_event

        mock_subs.return_value = [{"id": 1, "workspace_id": 10, "filters": {}, "delivery_mode": "instant"}]

        ids = enqueue_listing_event(
            raw_id=5,
            event_type="invalid_type",
            listing_data={"price": 1500},
        )

        assert ids == []
