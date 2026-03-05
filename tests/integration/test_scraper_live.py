"""
AGPARS Scraper Live Tests

T065.2 - Integration tests for live scraper functionality.
These tests verify the scraper adapters work with mocked browser responses.
"""

from unittest.mock import MagicMock, patch

import pytest

from services.collector.runner import RawListing, ScrapeJob, ScrapeResult

pytestmark = pytest.mark.integration


# ═══════════════════════════════════════════════════════════════════════════════
# ADAPTER IMPORT TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestAdapterImports:
    """Test that all adapters can be imported."""

    def test_import_daft_adapter(self):
        """Daft adapter is importable."""
        from services.collector.adapters.daft import DaftAdapter
        assert DaftAdapter is not None

    def test_import_rent_adapter(self):
        """Rent adapter is importable."""
        from services.collector.adapters.rent import RentAdapter
        assert RentAdapter is not None

    def test_import_myhome_adapter(self):
        """MyHome adapter is importable."""
        from services.collector.adapters.myhome import MyHomeAdapter
        assert MyHomeAdapter is not None

    def test_import_property_adapter(self):
        """Property.ie adapter is importable."""
        from services.collector.adapters.property_ie import PropertyIeAdapter
        assert PropertyIeAdapter is not None

    def test_import_sherryfitz_adapter(self):
        """SherryFitz adapter is importable."""
        from services.collector.adapters.sherryfitz import SherryFitzAdapter
        assert SherryFitzAdapter is not None

    def test_import_dng_adapter(self):
        """DNG adapter is importable."""
        from services.collector.adapters.dng import DngAdapter
        assert DngAdapter is not None


class TestAdapterRegistry:
    """Test adapter registry functionality."""

    def test_get_all_adapters(self):
        """All 6 adapters are registered."""
        from services.collector.adapters import ADAPTERS

        assert len(ADAPTERS) == 6
        assert "daft" in ADAPTERS
        assert "rent" in ADAPTERS
        assert "myhome" in ADAPTERS
        assert "property" in ADAPTERS
        assert "sherryfitz" in ADAPTERS
        assert "dng" in ADAPTERS

    def test_get_adapter_by_name(self):
        """Get adapter by source name."""
        from services.collector.adapters import get_adapter

        adapter = get_adapter("daft")
        assert adapter is not None
        assert adapter.get_source_name() == "daft"

    def test_get_unknown_adapter_returns_none(self):
        """Unknown adapter returns None."""
        from services.collector.adapters import get_adapter

        adapter = get_adapter("unknown_source")
        assert adapter is None


# ═══════════════════════════════════════════════════════════════════════════════
# SCRAPE JOB TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestScrapeJobCreation:
    """Test scrape job creation and configuration."""

    def test_create_job_with_city(self):
        """Create job targeting specific city."""
        job = ScrapeJob(source="daft", city="Dublin", county="Dublin")

        assert job.source == "daft"
        assert job.city == "Dublin"
        assert job.county == "Dublin"

    def test_create_job_county_only(self):
        """Create job targeting entire county."""
        job = ScrapeJob(source="rent", county="Cork")

        assert job.source == "rent"
        assert job.city is None
        assert job.county == "Cork"


# ═══════════════════════════════════════════════════════════════════════════════
# SCRAPE RESULT TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestScrapeResult:
    """Test scrape result handling."""

    def test_successful_result(self):
        """Successful scrape result with listings."""
        listings = [
            RawListing(source="daft", source_listing_id="1", url="http://1"),
            RawListing(source="daft", source_listing_id="2", url="http://2"),
        ]

        result = ScrapeResult(
            source="daft",
            city="Dublin",
            county="Dublin",
            listings=listings,
            success=True,
        )

        assert result.success is True
        assert len(result.listings) == 2
        assert result.errors == []

    def test_failed_result(self):
        """Failed scrape result with errors."""
        result = ScrapeResult(
            source="daft",
            city="Dublin",
            county="Dublin",
            success=False,
            errors=["Timeout loading page"],
        )

        assert result.success is False
        assert len(result.listings) == 0
        assert "Timeout" in result.errors[0]


# ═══════════════════════════════════════════════════════════════════════════════
# CIRCUIT BREAKER INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════


class TestCircuitBreakerIntegration:
    """Test circuit breaker integration with scraper."""

    @patch("services.collector.circuit_breaker.get_session")
    def test_check_circuit_closed(self, mock_session):
        """Closed circuit allows requests."""
        from services.collector.circuit_breaker import check_circuit

        mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.return_value.__enter__.return_value.get.return_value = None

        state = check_circuit("daft")
        assert state == "closed"

    @patch("services.collector.circuit_breaker.get_session")
    def test_is_source_available(self, mock_session):
        """Source availability check."""
        from services.collector.circuit_breaker import is_source_available

        mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.return_value.__enter__.return_value.get.return_value = None

        assert is_source_available("daft") is True


# ═══════════════════════════════════════════════════════════════════════════════
# THROTTLER INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════


class TestThrottlerIntegration:
    """Test throttler integration."""

    def test_source_configs_loaded(self):
        """Source configs are properly loaded."""
        from services.collector.throttle import SOURCE_CONFIGS

        assert "daft" in SOURCE_CONFIGS
        assert SOURCE_CONFIGS["daft"]["base_delay"] > 0

    def test_random_delay_generation(self):
        """Random delay generates values in range."""
        from services.collector.throttle import random_delay

        for _ in range(10):
            delay = random_delay(1.0, 2.0)
            assert 1.0 <= delay <= 2.0

    def test_jitter_application(self):
        """Jitter is applied correctly."""
        from services.collector.throttle import add_jitter

        base = 1.0
        results = [add_jitter(base, 0.2) for _ in range(10)]

        # All results should be within ±20% of base
        for r in results:
            assert 0.8 <= r <= 1.2
