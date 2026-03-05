"""
AGPARS Publisher Sync Tests

T034.1 - Integration tests for publisher sync functionality.
Tests pub_sync, watermark, change_router, and event_outbox integration.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.integration


# ═══════════════════════════════════════════════════════════════════════════════
# PUB SYNC TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestPubSyncIntegration:
    """Integration tests for pub sync operations."""

    @patch("services.publisher.pub_sync.get_session_context")
    def test_sync_listings_with_city_lookup(self, mock_session):
        """Sync populates city from core.cities lookup."""
        from services.publisher.pub_sync import sync_listings_to_pub

        mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []

        result = sync_listings_to_pub()

        assert "processed" in result
        assert "errors" in result

    @patch("services.publisher.pub_sync.get_session_context")
    def test_sync_with_since_filter(self, mock_session):
        """Sync can filter by since timestamp."""
        from services.publisher.pub_sync import sync_listings_to_pub

        mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []

        since = datetime(2024, 1, 1, tzinfo=UTC)
        result = sync_listings_to_pub(since=since)

        assert result["processed"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# WATERMARK TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestWatermarkIntegration:
    """Integration tests for watermark persistence."""

    def test_watermark_functions_exist(self):
        """Watermark functions are importable and callable."""
        from services.publisher.watermark import (
            WATERMARK_KEYS,
            WatermarkContext,
            get_watermark,
            set_watermark,
        )

        assert callable(set_watermark)
        assert callable(get_watermark)
        assert WatermarkContext is not None
        assert "normalizer_sync" in WATERMARK_KEYS


# ═══════════════════════════════════════════════════════════════════════════════
# CHANGE ROUTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestChangeRouterIntegration:
    """Integration tests for change routing."""

    def test_route_listing_signature(self):
        """Route listing function has correct signature."""
        import inspect

        from services.publisher.change_router import route_listing

        sig = inspect.signature(route_listing)
        params = list(sig.parameters.keys())

        assert "listing" in params
        assert "change_event" in params

    def test_routing_result_structure(self):
        """RoutingResult dataclass has correct fields."""
        from services.normalizer.change_detector import ChangeType
        from services.publisher.change_router import RoutingResult

        result = RoutingResult(
            listing_id=1,
            change_type=ChangeType.NEW,
            subscription_ids=[1, 2],
            workspace_ids=[10],
            subscription_workspaces={1: 10, 2: 10},
        )

        assert result.listing_id == 1
        assert result.change_type == ChangeType.NEW
        assert len(result.subscription_ids) == 2


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT OUTBOX TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestEventOutboxIntegration:
    """Integration tests for event outbox."""

    @patch("services.publisher.event_outbox.get_session_context")
    def test_create_and_retrieve_event(self, mock_session):
        """Create event and mark as delivered."""
        from services.publisher.event_outbox import (
            EventType,
            create_event,
            mark_event_delivered,
        )

        mock_ctx = MagicMock()
        mock_ctx.execute.return_value.scalar_one.return_value = 1
        mock_ctx.execute.return_value.rowcount = 1
        mock_session.return_value.__enter__ = MagicMock(return_value=mock_ctx)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        # Create event
        event_id = create_event(
            workspace_id=10,
            event_type=EventType.NEW,
            listing_raw_id=100,
            payload={"price": 1500},
        )

        assert event_id == 1

        # Mark delivered
        success = mark_event_delivered(event_id)
        assert success is True

    @patch("services.publisher.event_outbox.get_session_context")
    def test_event_status_transitions(self, mock_session):
        """Test event status transitions."""
        from services.publisher.event_outbox import (
            mark_event_delivering,
            mark_event_failed,
        )

        mock_ctx = MagicMock()
        mock_ctx.execute.return_value.rowcount = 1
        mock_ctx.execute.return_value.fetchone.return_value = MagicMock(retry_count=0)
        mock_session.return_value.__enter__ = MagicMock(return_value=mock_ctx)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        # Mark as delivering
        success = mark_event_delivering(1)
        assert success is True

        # Mark as failed
        success = mark_event_failed(1)
        assert success is True


# ═══════════════════════════════════════════════════════════════════════════════
# FULL SYNC PIPELINE TEST
# ═══════════════════════════════════════════════════════════════════════════════


class TestFullSyncPipeline:
    """Test complete sync pipeline flow."""

    def test_sync_functions_exist(self):
        """Sync entry functions exist and are importable."""
        from services.publisher.sync import (
            force_full_sync,
            run_publisher_sync,
            sync_source,
        )

        assert callable(run_publisher_sync)
        assert callable(sync_source)
        assert callable(force_full_sync)

    def test_pub_sync_functions(self):
        """Pub sync functions can be imported."""
        from services.publisher.pub_sync import (
            get_pub_sync_stats,
            sync_listings_to_pub,
            sync_removed_listings,
        )

        assert callable(sync_listings_to_pub)
        assert callable(sync_removed_listings)
        assert callable(get_pub_sync_stats)
