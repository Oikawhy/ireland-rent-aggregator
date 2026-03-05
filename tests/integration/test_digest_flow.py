"""
AGPARS Digest Flow Integration Tests

Tests for T067: digest aggregation and batch delivery.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestDigestFlow:
    """Integration tests for digest batching and delivery."""

    @patch("services.notifier.digest_scheduler.get_active_subscriptions")
    def test_collect_digest_subscriptions(self, mock_subs):
        """Only digest-mode subscriptions with due schedules are collected."""
        from services.notifier.digest_scheduler import get_due_digest_subscriptions
        from datetime import datetime

        mock_subs.return_value = [
            {
                "id": 1,
                "workspace_id": 10,
                "delivery_mode": "digest",
                "digest_schedule": {"frequency": "daily", "hour": 9, "minute": 0},
            },
            {
                "id": 2,
                "workspace_id": 20,
                "delivery_mode": "instant",
                "digest_schedule": None,
            },
        ]

        now = datetime(2025, 1, 15, 9, 0)
        due = get_due_digest_subscriptions(now)

        # Only subscription 1 should be included (digest mode + due)
        assert len(due) == 1
        assert due[0]["id"] == 1

    @patch("packages.telegram.digest.get_pending_events_for_workspace")
    def test_batch_creation(self, mock_events):
        """Events are batched into a single digest message."""
        from packages.telegram.digest import create_digest_batch

        mock_events.return_value = [
            {"id": 1, "event_type": "new", "payload": {"price": 1500, "city": "Dublin"}},
            {"id": 2, "event_type": "new", "payload": {"price": 2000, "city": "Cork"}},
            {"id": 3, "event_type": "updated", "payload": {"price": 1800, "city": "Dublin"}},
        ]

        batch = create_digest_batch(workspace_id=10)

        assert batch["total_events"] == 3
        assert batch["new_count"] == 2
        assert batch["updated_count"] == 1
        assert "message" in batch

    @patch("packages.telegram.digest.get_pending_events_for_workspace")
    def test_empty_digest_skipped(self, mock_events):
        """Empty digest (no events) returns None."""
        from packages.telegram.digest import create_digest_batch

        mock_events.return_value = []
        batch = create_digest_batch(workspace_id=10)
        assert batch is None
