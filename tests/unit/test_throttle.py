"""
Tests for services.collector.throttle module.

Tests rate limiting, jitter, and throttle functions.
"""


from services.collector.throttle import (
    DEFAULT_RATE_LIMIT,
    SOURCE_RATE_LIMITS,
    Throttler,
    add_jitter,
    random_delay,
)


class TestConstants:
    """Tests for throttle constants."""

    def test_source_rate_limits_defined(self):
        """Test that source rate limits are defined."""
        assert isinstance(SOURCE_RATE_LIMITS, dict)
        assert len(SOURCE_RATE_LIMITS) > 0

    def test_source_rate_limits_structure(self):
        """Test that rate limits have expected structure."""
        for source, config in SOURCE_RATE_LIMITS.items():
            assert "rpm" in config or "min_delay_ms" in config or "base_delay" in config
            assert isinstance(source, str)

    def test_default_rate_limit_defined(self):
        """Test that default rate limit is defined."""
        assert isinstance(DEFAULT_RATE_LIMIT, dict)
        assert "rpm" in DEFAULT_RATE_LIMIT or "base_delay" in DEFAULT_RATE_LIMIT

    def test_known_sources_have_limits(self):
        """Test that known sources are configured."""
        expected_sources = ["daft", "rent", "myhome", "property", "sherryfitz", "dng"]
        for source in expected_sources:
            assert source in SOURCE_RATE_LIMITS, f"Missing rate limit for {source}"


class TestRandomDelay:
    """Tests for random_delay function."""

    def test_returns_float(self):
        """Test that random_delay returns a float."""
        result = random_delay(0.1, 0.5)
        assert isinstance(result, float)

    def test_within_range(self):
        """Test that random_delay is within specified range."""
        min_val = 1.0
        max_val = 2.0

        for _ in range(100):
            result = random_delay(min_val, max_val)
            assert min_val <= result <= max_val

    def test_same_min_max(self):
        """Test with same min and max."""
        result = random_delay(1.0, 1.0)
        assert result == 1.0


class TestAddJitter:
    """Tests for add_jitter function."""

    def test_returns_float(self):
        """Test that add_jitter returns a float."""
        result = add_jitter(1.0)
        assert isinstance(result, float)

    def test_jitter_within_range(self):
        """Test that jitter stays within expected range."""
        base = 1.0
        jitter_percent = 0.2

        for _ in range(100):
            result = add_jitter(base, jitter_percent)
            # Should be within ±20% of base
            assert base * 0.8 <= result <= base * 1.2

    def test_zero_jitter(self):
        """Test with zero jitter."""
        base = 5.0
        result = add_jitter(base, 0.0)
        assert result == base

    def test_high_jitter(self):
        """Test with high jitter percentage."""
        base = 1.0
        jitter_percent = 0.5  # ±50%

        results = [add_jitter(base, jitter_percent) for _ in range(100)]
        assert min(results) >= base * 0.5
        assert max(results) <= base * 1.5


class TestThrottler:
    """Tests for Throttler class."""

    def test_throttler_creation(self):
        """Test Throttler can be instantiated."""
        throttler = Throttler()
        assert throttler is not None

    def test_get_rate_limit_known_source(self):
        """Test getting rate limit for known source."""
        throttler = Throttler()
        config = throttler._get_rate_limit("daft")

        assert isinstance(config, dict)

    def test_get_rate_limit_unknown_source(self):
        """Test getting rate limit for unknown source uses default."""
        throttler = Throttler()
        config = throttler._get_rate_limit("unknown_source")

        assert isinstance(config, dict)

    def test_apply_jitter(self):
        """Test _apply_jitter method."""
        throttler = Throttler()
        base_delay = 1000  # ms

        for _ in range(100):
            result = throttler._apply_jitter(base_delay)
            # Should apply some jitter (0.5x to 1.5x)
            assert 500 <= result <= 1500

    def test_apply_jitter_zero(self):
        """Test _apply_jitter with zero delay."""
        throttler = Throttler()
        result = throttler._apply_jitter(0)
        assert result >= 0
