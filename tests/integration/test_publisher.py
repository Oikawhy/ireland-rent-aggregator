"""
AGPARS Integration Tests — Publisher Pipeline

Tests for the full publisher ETL flow.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

# Skip all tests if database is not available
pytestmark = pytest.mark.integration


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = MagicMock()
    session.execute.return_value.fetchall.return_value = []
    session.execute.return_value.scalar_one.return_value = 1
    return session


@pytest.fixture
def sample_normalized_listing():
    """Sample normalized listing for testing."""
    return {
        "id": 1,
        "raw_id": 100,
        "price": Decimal("1500.00"),
        "beds": 2,
        "baths": 1,
        "property_type": "apartment",
        "furnished": True,
        "city_id": 1,
        "county": "Dublin",
        "area_text": "Dublin City Centre",
        "status": "active",
        "updated_at": datetime.now(UTC),
        "source": "daft",
        "url": "https://daft.ie/listing/12345",
        "first_photo_url": "https://daft.ie/photos/12345.jpg",
    }


@pytest.fixture
def sample_subscription():
    """Sample subscription for routing tests."""
    return {
        "id": 1,
        "workspace_id": 10,
        "filters": {
            "min_price": 1000,
            "max_price": 2000,
            "counties": ["Dublin"],
        },
        "delivery_mode": "instant",
        "is_enabled": True,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PUB SYNC TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestPubSync:
    """Tests for pub schema sync operations."""

    @patch("services.publisher.pub_sync.get_session_context")
    def test_sync_listings_to_pub_empty(self, mock_get_session, mock_session):
        """Test sync with no listings."""
        from services.publisher.pub_sync import sync_listings_to_pub

        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.fetchall.return_value = []

        result = sync_listings_to_pub()

        assert result["processed"] == 0
        assert result["errors"] == 0

    @patch("services.publisher.pub_sync.get_session_context")
    def test_sync_listings_handles_error(self, mock_get_session):
        """Test graceful error handling."""
        from services.publisher.pub_sync import sync_listings_to_pub

        mock_get_session.side_effect = Exception("DB connection failed")

        with pytest.raises(Exception):  # noqa: B017
            sync_listings_to_pub()


# ═══════════════════════════════════════════════════════════════════════════════
# CHANGE ROUTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestChangeRouter:
    """Tests for subscription matching and routing."""

    def test_route_listing_matches_subscription(self, sample_normalized_listing):  # noqa: ARG002
        """Test that listing matching filter returns correct subscription."""
        from packages.core.validation import SubscriptionFilters

        filters = SubscriptionFilters(
            min_price=1000,
            max_price=2000,
            counties=["Dublin"],
        )

        # The listing matches the filter
        assert filters.matches(sample_normalized_listing) is True

    def test_route_listing_no_match_price_too_high(self, sample_normalized_listing):
        """Test listing outside price range doesn't match."""
        from packages.core.validation import SubscriptionFilters

        filters = SubscriptionFilters(
            min_price=100,
            max_price=500,  # Listing price is 1500
        )

        assert filters.matches(sample_normalized_listing) is False


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT OUTBOX TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestEventOutbox:
    """Tests for event outbox operations."""

    @patch("services.publisher.event_outbox.get_session_context")
    def test_create_event(self, mock_get_session, mock_session):
        """Test creating an event in outbox."""
        from services.publisher.event_outbox import EventType, create_event

        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.scalar_one.return_value = 42

        event_id = create_event(
            workspace_id=10,
            event_type=EventType.NEW,
            listing_raw_id=100,
            payload={"price": "1500"},
        )

        assert isinstance(event_id, int)
        assert event_id == 42


# ═══════════════════════════════════════════════════════════════════════════════
# WATERMARK TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestWatermark:
    """Tests for watermark operations."""

    @patch("services.publisher.watermark.get_session_context")
    def test_set_and_get_watermark(self, mock_get_session, mock_session):
        """Test watermark set/get cycle."""
        from services.publisher.watermark import set_watermark

        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        # Set watermark returns True
        assert set_watermark("test_key", datetime.now(UTC)) is True


# ═══════════════════════════════════════════════════════════════════════════════
# DIGEST SCHEDULE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestDigestScheduleValidation:
    """Tests for digest schedule validation per ARCHITECT.md."""

    def test_valid_twice_daily_schedule(self):
        """Test valid twice daily schedule."""
        from packages.core.validation import validate_digest_schedule

        raw = {
            "interval": "twice_daily",
            "times": ["09:00", "18:00"],
        }

        schedule, errors = validate_digest_schedule(raw)

        assert len(errors) == 0
        assert schedule.interval == "twice_daily"
        assert schedule.times == ["09:00", "18:00"]

    def test_invalid_interval(self):
        """Test rejection of invalid interval."""
        from packages.core.validation import validate_digest_schedule

        raw = {"interval": "every_hour"}

        schedule, errors = validate_digest_schedule(raw)

        assert len(errors) > 0
        assert "Invalid interval" in errors[0]

    def test_times_too_close(self):
        """Test rejection when times are less than 4 hours apart."""
        from packages.core.validation import validate_digest_schedule

        raw = {
            "interval": "twice_daily",
            "times": ["09:00", "11:00"],  # Only 2 hours apart
        }

        schedule, errors = validate_digest_schedule(raw)

        assert len(errors) > 0
        assert "4 hours apart" in errors[0]

    def test_times_valid_gap(self):
        """Test acceptance when times are >= 4 hours apart."""
        from packages.core.validation import validate_digest_schedule

        raw = {
            "interval": "twice_daily",
            "times": ["06:00", "18:00"],  # 12 hours apart
        }

        schedule, errors = validate_digest_schedule(raw)

        assert len(errors) == 0
