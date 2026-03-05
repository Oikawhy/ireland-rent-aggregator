"""
Tests for Publisher Sync

T034.1 - Tests for services/publisher/ components
"""


from services.publisher.event_outbox import EventStatus, EventType

# ═══════════════════════════════════════════════════════════════════════════════
# CHANGE ROUTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestChangeRouter:
    """Tests for change router functionality."""

    def test_routing_result_structure(self):
        """RoutingResult has correct structure."""
        from services.normalizer.change_detector import ChangeType
        from services.publisher.change_router import RoutingResult

        result = RoutingResult(
            listing_id=123,
            change_type=ChangeType.NEW,
            subscription_ids=[1, 2, 3],
            workspace_ids=[10, 20],
            subscription_workspaces={1: 10, 2: 10, 3: 20},
        )

        assert result.listing_id == 123
        assert result.change_type == ChangeType.NEW
        assert len(result.subscription_ids) == 3
        assert len(result.workspace_ids) == 2

    def test_routing_stats(self):
        """Get routing statistics."""
        from services.normalizer.change_detector import ChangeType
        from services.publisher.change_router import RoutingResult, get_routing_stats

        results = [
            RoutingResult(1, ChangeType.NEW, [1, 2], [10], {1: 10, 2: 10}),
            RoutingResult(2, ChangeType.NEW, [1], [10], {1: 10}),
            RoutingResult(3, ChangeType.NEW, [], [], {}),  # Unrouted
        ]

        stats = get_routing_stats(results)
        assert stats["total_listings"] == 3
        assert stats["routed"] == 2
        assert stats["unrouted"] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT OUTBOX TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestEventOutbox:
    """Tests for event outbox functionality."""

    def test_event_types(self):
        """Event types are defined correctly."""
        # Per migration schema: only NEW and UPDATED
        assert EventType.NEW.value == "new"
        assert EventType.UPDATED.value == "updated"
        # Verify only 2 event types exist
        assert len(EventType) == 2

    def test_event_statuses(self):
        """Event statuses are defined correctly."""
        # Per migration schema: pending, delivering, delivered, failed, dead
        assert EventStatus.PENDING.value == "pending"
        assert EventStatus.DELIVERING.value == "delivering"
        assert EventStatus.DELIVERED.value == "delivered"
        assert EventStatus.FAILED.value == "failed"
        assert EventStatus.DEAD.value == "dead"
        assert len(EventStatus) == 5


# ═══════════════════════════════════════════════════════════════════════════════
# WATERMARK TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestWatermark:
    """Tests for watermark functionality."""

    def test_watermark_keys(self):
        """Watermark keys are defined."""
        from services.publisher.watermark import WATERMARK_KEYS

        assert "normalizer_sync" in WATERMARK_KEYS
        assert "publisher_sync" in WATERMARK_KEYS
        assert "collector_daft" in WATERMARK_KEYS

    def test_watermark_context_manager(self):
        """WatermarkContext can be instantiated."""
        from services.publisher.watermark import WatermarkContext

        ctx = WatermarkContext("test_key")
        assert ctx.key == "test_key"
        assert ctx.update_on_success is True


# ═══════════════════════════════════════════════════════════════════════════════
# PUB SYNC TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestPubSync:
    """Tests for pub schema sync."""

    def test_sync_function_exists(self):
        """Sync functions exist and can be imported."""
        from services.publisher.pub_sync import (
            get_pub_sync_stats,
            sync_listings_to_pub,
            sync_removed_listings,
        )

        assert callable(sync_listings_to_pub)
        assert callable(sync_removed_listings)
        assert callable(get_pub_sync_stats)


# ═══════════════════════════════════════════════════════════════════════════════
# SYNC ENTRY POINT TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestSyncEntry:
    """Tests for sync entry point."""

    def test_sync_functions_exist(self):
        """Sync entry functions exist."""
        from services.publisher.sync import (
            force_full_sync,
            run_publisher_sync,
            sync_source,
        )

        assert callable(run_publisher_sync)
        assert callable(sync_source)
        assert callable(force_full_sync)

    def test_event_type_mapping(self):
        """Event type mapping works."""
        from services.normalizer.change_detector import ChangeType
        from services.publisher.event_outbox import EventType
        from services.publisher.sync import _map_change_to_event_type

        # Per schema: only NEW and UPDATED exist
        # NEW → NEW, all others → UPDATED
        assert _map_change_to_event_type(ChangeType.NEW) == EventType.NEW
        assert _map_change_to_event_type(ChangeType.PRICE_CHANGED) == EventType.UPDATED
        assert _map_change_to_event_type(ChangeType.REMOVED) == EventType.UPDATED

    def test_build_event_payload(self):
        """Event payload is built correctly."""
        from services.normalizer.change_detector import ChangeEvent, ChangeType
        from services.publisher.sync import _build_event_payload

        listing = {
            "id": 123,
            "source": "daft",
            "url": "https://daft.ie/123",
            "price": 1500,
            "beds": 2,
            "city_id": 456,
        }
        event = ChangeEvent(
            change_type=ChangeType.NEW,
            field=None,
            old_value=None,
            new_value=None,
        )

        payload = _build_event_payload(listing, event)

        assert payload["listing_id"] == 123
        assert payload["source"] == "daft"
        assert payload["url"] == "https://daft.ie/123"
        assert payload["change_type"] == "new"
