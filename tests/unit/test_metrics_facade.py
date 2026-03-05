"""
Tests for MetricsFacade

P2-01 - Tests for real MetricsFacade implementation
"""

import pytest

from packages.observability.metrics import (
    KNOWN_COUNTERS,
    KNOWN_GAUGES,
    MetricsFacade,
    get_metrics,
)


class TestMetricsFacade:
    """Tests for MetricsFacade class."""

    def test_counter_increments_known_metric(self):
        """Test that counter increments a known metric."""
        facade = MetricsFacade()
        # Should not raise - listings_excluded_total is a known counter
        facade.counter("listings_excluded_total", labels={"reason": "test", "source": "test"})

    def test_counter_raises_for_unknown_metric(self):
        """Test that counter raises KeyError for unknown metrics."""
        facade = MetricsFacade()
        with pytest.raises(KeyError, match="Unknown counter metric"):
            facade.counter("unknown_metric_name")

    def test_gauge_sets_known_metric(self):
        """Test that gauge sets a known metric."""
        facade = MetricsFacade()
        # Should not raise - circuit_breaker_state is a known gauge
        facade.gauge("circuit_breaker_state", 1.0, labels={"source": "test"})

    def test_gauge_raises_for_unknown_metric(self):
        """Test that gauge raises KeyError for unknown metrics."""
        facade = MetricsFacade()
        with pytest.raises(KeyError, match="Unknown gauge metric"):
            facade.gauge("unknown_gauge_name", 1.0)

    def test_increment_is_alias_for_counter(self):
        """Test that increment calls counter."""
        facade = MetricsFacade()
        # Should work the same as counter()
        facade.increment("scrape_errors_total", labels={"source": "test", "reason": "timeout"})

    def test_get_metrics_returns_singleton(self):
        """Test that get_metrics returns a MetricsFacade instance."""
        metrics = get_metrics()
        assert isinstance(metrics, MetricsFacade)

    def test_known_counters_registry_populated(self):
        """Test that KNOWN_COUNTERS contains expected metrics."""
        assert "listings_excluded_total" in KNOWN_COUNTERS
        assert "scrape_errors_total" in KNOWN_COUNTERS
        assert "listings_found_total" in KNOWN_COUNTERS

    def test_known_gauges_registry_populated(self):
        """Test that KNOWN_GAUGES contains expected metrics."""
        assert "circuit_breaker_state" in KNOWN_GAUGES
        assert "outbox_pending" in KNOWN_GAUGES
        assert "publisher_lag_seconds" in KNOWN_GAUGES
