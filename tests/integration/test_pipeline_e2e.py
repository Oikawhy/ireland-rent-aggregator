"""
AGPARS End-to-End Pipeline Test

Tests the complete data flow: Collector → Normalizer → Publisher
"""

from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.integration


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE COMPONENT IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestPipelineImports:
    """Verify all pipeline components are importable."""

    def test_collector_components(self):
        """Collector components are importable."""
        from services.collector.adapters import ADAPTERS
        from services.collector.runner import ScrapeJob

        assert ScrapeJob is not None
        assert len(ADAPTERS) == 6

    def test_normalizer_components(self):
        """Normalizer components are importable."""
        from services.normalizer.normalize import NormalizationPipeline

        assert NormalizationPipeline is not None
        # Create pipeline and verify normalize method exists
        pipeline = NormalizationPipeline()
        assert hasattr(pipeline, 'normalize')

    def test_publisher_components(self):
        """Publisher components are importable."""
        from services.publisher.event_outbox import EventType
        from services.publisher.pub_sync import sync_listings_to_pub

        assert sync_listings_to_pub is not None
        assert EventType.NEW.value == "new"


# ═══════════════════════════════════════════════════════════════════════════════
# DATA TRANSFORMATION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestDataTransformation:
    """Test data transformation through pipeline stages."""

    def test_raw_to_normalized(self):
        """Raw listing transforms to normalized format."""
        from services.collector.runner import RawListing
        from services.collector.sanitize import sanitize_beds, sanitize_price

        raw = RawListing(
            source="daft",
            source_listing_id="12345",
            url="https://daft.ie/12345",
            price_text="€1,800 per month",
            beds_text="2 beds",
            baths_text="1 bathroom",
            location_text="Dublin 4, Dublin",
        )

        # Test sanitization
        price = sanitize_price(raw.price_text)
        beds = sanitize_beds(raw.beds_text)

        assert price == 1800
        assert beds == 2

    def test_exclusion_rules_applied(self):
        """Exclusion rules filter out invalid listings."""
        from services.rules.exclusions import ExclusionEngine

        engine = ExclusionEngine()

        # Create mock raw listing objects
        class MockRawListing:
            def __init__(self, title, description, location):
                self.title = title
                self.description = description
                self.location_text = location

        # Student housing should be excluded
        student_listing = MockRawListing(
            title="Student Accommodation UCD",
            description="Near campus",
            location="Dublin",
        )
        result = engine.check_exclusion(student_listing)
        assert result is not None
        assert result.is_excluded is True
        # Check 'rule' field instead of 'reason' (reason is human-readable text)
        assert result.rule == "student_housing"

        # Normal listing should pass
        normal_listing = MockRawListing(
            title="2 Bed Apartment",
            description="Lovely apartment",
            location="Dublin 4",
        )
        result = engine.check_exclusion(normal_listing)
        assert result.is_excluded is False


# ═══════════════════════════════════════════════════════════════════════════════
# MOCKED E2E FLOW
# ═══════════════════════════════════════════════════════════════════════════════


class TestMockedE2EFlow:
    """Test E2E flow with mocked database."""

    @patch("services.publisher.pub_sync.get_session_context")
    @patch("packages.storage.listings.get_session")
    def test_collect_normalize_publish_flow(self, mock_listings_session, mock_pub_session):
        """Simulate full pipeline flow."""
        from services.collector.runner import RawListing, ScrapeResult
        from services.collector.sanitize import sanitize_beds, sanitize_price

        # Setup mocks
        for mock in [mock_listings_session, mock_pub_session]:
            mock_ctx = MagicMock()
            mock_ctx.execute.return_value.fetchall.return_value = []
            mock_ctx.execute.return_value.scalar_one.return_value = 1
            mock.return_value.__enter__ = MagicMock(return_value=mock_ctx)
            mock.return_value.__exit__ = MagicMock(return_value=False)

        # 1. COLLECT: Create raw listings
        raw_listings = [
            RawListing(
                source="daft",
                source_listing_id="1",
                url="https://daft.ie/1",
                price_text="€1,500",
                beds_text="2",
                baths_text="1",
                location_text="Dublin 4",
            ),
            RawListing(
                source="daft",
                source_listing_id="2",
                url="https://daft.ie/2",
                price_text="€1,800",
                beds_text="3",
                location_text="Cork City",
            ),
        ]

        result = ScrapeResult(
            source="daft",
            city="Dublin",
            county="Dublin",
            listings=raw_listings,
            success=True,
        )

        assert result.success
        assert len(result.listings) == 2

        # 2. NORMALIZE: Sanitize raw data
        normalized = []
        for raw in raw_listings:
            normalized.append({
                "raw_id": int(raw.source_listing_id),
                "price": sanitize_price(raw.price_text),
                "beds": sanitize_beds(raw.beds_text),
                "location": raw.location_text,
            })

        assert normalized[0]["price"] == 1500
        assert normalized[1]["beds"] == 3

        # 3. PUBLISH: Data would go to pub schema (mocked)
        from services.publisher.pub_sync import sync_listings_to_pub

        sync_result = sync_listings_to_pub()
        assert "processed" in sync_result


# ═══════════════════════════════════════════════════════════════════════════════
# WATERMARK CONTINUITY
# ═══════════════════════════════════════════════════════════════════════════════


class TestWatermarkContinuity:
    """Test watermark-based incremental processing."""

    def test_watermark_imports(self):
        """Watermark module imports and WatermarkContext works."""
        from services.publisher.watermark import (
            WatermarkContext,
            with_watermark,
        )

        # Verify WatermarkContext can be instantiated
        ctx = WatermarkContext("test_key")
        assert ctx.key == "test_key"
        assert ctx.update_on_success is True

        # Verify with_watermark creates a context
        wm_ctx = with_watermark("pub_sync")
        assert wm_ctx.key == "pub_sync"


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT CREATION FOR NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════════════════════


class TestEventCreation:
    """Test event creation for notification delivery."""

    @patch("services.publisher.event_outbox.get_session_context")
    def test_new_listing_creates_event(self, mock_session):
        """New listing creates event in outbox."""
        from services.publisher.event_outbox import EventType, create_event

        mock_ctx = MagicMock()
        mock_ctx.execute.return_value.scalar_one.return_value = 42
        mock_session.return_value.__enter__ = MagicMock(return_value=mock_ctx)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        event_id = create_event(
            workspace_id=1,
            event_type=EventType.NEW,
            listing_raw_id=100,
            payload={
                "price": 1500,
                "beds": 2,
                "city": "Dublin",
            },
        )

        assert event_id == 42

    @patch("services.publisher.event_outbox.get_session_context")
    def test_updated_listing_creates_event(self, mock_session):
        """Updated listing creates event in outbox."""
        from services.publisher.event_outbox import EventType, create_event

        mock_ctx = MagicMock()
        mock_ctx.execute.return_value.scalar_one.return_value = 43
        mock_session.return_value.__enter__ = MagicMock(return_value=mock_ctx)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        event_id = create_event(
            workspace_id=1,
            event_type=EventType.UPDATED,
            listing_raw_id=100,
            payload={
                "old_price": 1500,
                "new_price": 1400,
            },
        )

        assert event_id == 43
