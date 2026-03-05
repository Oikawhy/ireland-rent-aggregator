"""
AGPARS Delivery Flow Integration Tests

End-to-end tests for T029: enqueue → worker → delivery.
Uses mocks for Telegram API and database.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ═══════════════════════════════════════════════════════════════════════════════
# E2E DELIVERY PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════


class TestDeliveryFlow:
    """Integration test: outbox enqueue → worker process → telegram delivery."""

    @pytest.mark.asyncio
    @patch("services.notifier.outbox_worker.send_notification", new_callable=AsyncMock)
    @patch("services.notifier.outbox_worker.render_card")
    @patch("services.notifier.outbox_worker.get_workspace")
    @patch("services.notifier.outbox_worker.RateLimiter")
    def test_full_delivery_pipeline(
        self,
        MockRateLimiter,
        mock_get_workspace,
        mock_render_card,
        mock_send,
    ):
        """Test: pending event → worker picks up → renders → sends → delivered."""
        import asyncio
        from services.notifier.outbox_worker import OutboxWorker

        # Setup mocks
        rate_limiter = MagicMock()
        rate_limiter.wait_for_slot.return_value = True
        MockRateLimiter.return_value = rate_limiter

        mock_get_workspace.return_value = {
            "id": 1,
            "tg_chat_id": 12345,
            "is_active": True,
        }

        mock_render_card.return_value = "🏠 *New Listing*\n€1,500/month"
        mock_send.return_value = True

        worker = OutboxWorker(batch_size=10)

        # Mock _fetch_pending_events and _mark_delivered
        test_event = {
            "id": 100,
            "workspace_id": 1,
            "event_type": "new",
            "listing_raw_id": 42,
            "payload": {"price": 1500, "city": "Dublin", "county": "Dublin"},
            "retry_count": 0,
            "created_at": "2025-01-01T00:00:00",
        }

        with patch.object(worker, "_fetch_pending_events", return_value=[test_event]), \
             patch.object(worker, "_mark_delivered") as mock_mark, \
             patch.object(worker, "_mark_failed"):

            processed = asyncio.get_event_loop().run_until_complete(
                worker.process_batch()
            )

        assert processed == 1
        mock_send.assert_called_once()
        mock_mark.assert_called_once_with(100)

    @pytest.mark.asyncio
    @patch("services.notifier.outbox_worker.send_notification", new_callable=AsyncMock)
    @patch("services.notifier.outbox_worker.render_card")
    @patch("services.notifier.outbox_worker.get_workspace")
    @patch("services.notifier.outbox_worker.RateLimiter")
    def test_failed_delivery_marks_failed(
        self,
        MockRateLimiter,
        mock_get_workspace,
        mock_render_card,
        mock_send,
    ):
        """Test: delivery failure → event marked FAILED with retry increment."""
        import asyncio
        from services.notifier.outbox_worker import OutboxWorker

        rate_limiter = MagicMock()
        rate_limiter.wait_for_slot.return_value = True
        MockRateLimiter.return_value = rate_limiter

        mock_get_workspace.return_value = {
            "id": 1,
            "tg_chat_id": 12345,
            "is_active": True,
        }

        mock_render_card.return_value = "test message"
        mock_send.return_value = False  # Delivery fails

        worker = OutboxWorker()

        test_event = {
            "id": 200,
            "workspace_id": 1,
            "event_type": "new",
            "listing_raw_id": 50,
            "payload": {},
            "retry_count": 1,
            "created_at": "2025-01-01T00:00:00",
        }

        with patch.object(worker, "_fetch_pending_events", return_value=[test_event]), \
             patch.object(worker, "_mark_delivered") as mock_delivered, \
             patch.object(worker, "_mark_failed") as mock_failed:

            processed = asyncio.get_event_loop().run_until_complete(
                worker.process_batch()
            )

        assert processed == 1
        mock_delivered.assert_not_called()
        mock_failed.assert_called_once_with(200, 1)

    @pytest.mark.asyncio
    @patch("services.notifier.outbox_worker.RateLimiter")
    @patch("services.notifier.outbox_worker.get_workspace")
    def test_inactive_workspace_skipped(
        self,
        mock_get_workspace,
        MockRateLimiter,
    ):
        """Test: inactive workspace → event marked delivered (no retry)."""
        import asyncio
        from services.notifier.outbox_worker import OutboxWorker

        MockRateLimiter.return_value = MagicMock()

        mock_get_workspace.return_value = {
            "id": 1,
            "tg_chat_id": 12345,
            "is_active": False,  # Inactive
        }

        worker = OutboxWorker()

        test_event = {
            "id": 300,
            "workspace_id": 1,
            "event_type": "new",
            "listing_raw_id": 60,
            "payload": {},
            "retry_count": 0,
            "created_at": "2025-01-01T00:00:00",
        }

        with patch.object(worker, "_fetch_pending_events", return_value=[test_event]), \
             patch.object(worker, "_mark_delivered") as mock_delivered, \
             patch.object(worker, "_mark_failed"):

            processed = asyncio.get_event_loop().run_until_complete(
                worker.process_batch()
            )

        assert processed == 1
        # Inactive workspace → mark delivered to stop retrying
        mock_delivered.assert_called_once_with(300)


# ═══════════════════════════════════════════════════════════════════════════════
# SUBSCRIPTION FILTER → EVENT MATCHING
# ═══════════════════════════════════════════════════════════════════════════════


class TestSubscriptionMatching:
    """Test subscription filter matching for outbox enqueue."""

    @patch("services.notifier.outbox_enqueue._insert_event")
    @patch("services.notifier.outbox_enqueue.get_active_subscriptions")
    def test_multiple_matching_subscriptions(self, mock_subs, mock_insert):
        """Correct number of events for multiple matching workspaces."""
        from services.notifier.outbox_enqueue import enqueue_listing_event

        mock_subs.return_value = [
            {"id": 1, "workspace_id": 10, "filters": {}, "delivery_mode": "instant"},
            {"id": 2, "workspace_id": 20, "filters": {}, "delivery_mode": "instant"},
            {"id": 3, "workspace_id": 30, "filters": {"max_budget": 1000}, "delivery_mode": "instant"},
        ]

        mock_insert.side_effect = [101, 102]  # 2 matches (sub 3 won't match)

        ids = enqueue_listing_event(
            raw_id=5,
            event_type="new",
            listing_data={"price": 1500, "city_id": 1, "county": "Dublin"},
        )

        # Sub 1 and 2 match (no filters), sub 3 doesn't (max_budget=1000 < price 1500)
        assert len(ids) == 2
        assert mock_insert.call_count == 2


# ═══════════════════════════════════════════════════════════════════════════════
# RENDER CARD TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestRenderCard:
    """Test card rendering with lease-unknown label."""

    def test_new_listing_card(self):
        """New listing renders with template defaults."""
        from packages.telegram.render import render_card

        listing = {
            "price": 1500,
            "beds": 2,
            "city": "Dublin",
            "county": "Dublin",
            "url": "https://example.com/listing/1",
            "source": "daft",
        }

        result = render_card(listing, "new")

        assert "1,500" in result
        assert "Dublin" in result
        assert "example.com" in result

    def test_lease_unknown_label(self):
        """T063: lease_length_unknown=True shows Unknown label."""
        from packages.telegram.render import render_card

        listing = {
            "price": 1500,
            "beds": 2,
            "city": "Dublin",
            "county": "Dublin",
            "url": "https://example.com",
            "source": "daft",
            "lease_length_unknown": True,
        }

        result = render_card(listing, "new")
        assert "Lease" in result
        assert "Unknown" in result

    def test_lease_known_label(self):
        """Lease months shown when lease_length_months is set."""
        from packages.telegram.render import render_card

        listing = {
            "price": 1500,
            "beds": 2,
            "city": "Dublin",
            "county": "Dublin",
            "url": "https://example.com",
            "source": "daft",
            "lease_length_months": 12,
        }

        result = render_card(listing, "new")
        assert "1 year" in result

    def test_updated_listing_card(self):
        """Updated listing renders with changes."""
        from packages.telegram.render import render_card

        listing = {
            "price": 1600,
            "city": "Cork",
            "county": "Cork",
            "url": "https://example.com/listing/2",
            "changes": {"price": (1500, 1600)},
        }

        result = render_card(listing, "updated")
        assert "Updated" in result
        assert "1,500" in result
        assert "1,600" in result
