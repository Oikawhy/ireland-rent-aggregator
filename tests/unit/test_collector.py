"""
Tests for Collector Components

T065.1 - Tests for collector runner and related components
"""


from services.collector.runner import RawListing, ScrapeJob, ScrapeResult

# ═══════════════════════════════════════════════════════════════════════════════
# RAW LISTING TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestRawListing:
    """Tests for RawListing dataclass."""

    def test_raw_listing_creation(self):
        """Create a RawListing with required fields."""
        listing = RawListing(
            source="daft",
            source_listing_id="12345",
            url="https://daft.ie/property/12345",
        )

        assert listing.source == "daft"
        assert listing.source_listing_id == "12345"
        assert listing.url == "https://daft.ie/property/12345"

    def test_raw_listing_with_all_fields(self):
        """Create a RawListing with all fields."""
        listing = RawListing(
            source="daft",
            source_listing_id="12345",
            url="https://daft.ie/property/12345",
            first_photo_url="https://daft.ie/photo.jpg",
            raw_payload={"title": "Nice apartment"},
            title="Nice apartment",
            price_text="€1,500 per month",
            beds_text="2",
            baths_text="1",
            location_text="Dublin 4",
        )

        assert listing.first_photo_url == "https://daft.ie/photo.jpg"
        assert listing.title == "Nice apartment"
        assert listing.price_text == "€1,500 per month"

    def test_raw_listing_optional_fields_default(self):
        """Optional fields default to None."""
        listing = RawListing(
            source="rent",
            source_listing_id="456",
            url="https://rent.ie/456",
        )

        assert listing.first_photo_url is None
        assert listing.raw_payload == {}  # Defaults to empty dict
        assert listing.title is None


# ═══════════════════════════════════════════════════════════════════════════════
# SCRAPE JOB TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestScrapeJob:
    """Tests for ScrapeJob dataclass."""

    def test_scrape_job_with_source(self):
        """Create a ScrapeJob with source only."""
        job = ScrapeJob(source="daft")

        assert job.source == "daft"
        assert job.city is None
        assert job.county is None

    def test_scrape_job_with_city(self):
        """Create a ScrapeJob with city."""
        job = ScrapeJob(source="daft", city="Dublin")

        assert job.source == "daft"
        assert job.city == "Dublin"

    def test_scrape_job_with_county(self):
        """Create a ScrapeJob with county."""
        job = ScrapeJob(source="rent", county="Cork")

        assert job.source == "rent"
        assert job.county == "Cork"


# ═══════════════════════════════════════════════════════════════════════════════
# SCRAPE RESULT TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestScrapeResult:
    """Tests for ScrapeResult dataclass."""

    def test_successful_result(self):
        """Create a successful scrape result."""
        listings = [
            RawListing(source="daft", source_listing_id="1", url="http://1"),
            RawListing(source="daft", source_listing_id="2", url="http://2"),
        ]
        result = ScrapeResult(
            source="daft",
            city="Dublin",
            county="Dublin",
            success=True,
            listings=listings,
        )

        assert result.success is True
        assert len(result.listings) == 2
        assert result.errors == []

    def test_failed_result(self):
        """Create a failed scrape result."""
        result = ScrapeResult(
            source="daft",
            city=None,
            county=None,
            success=False,
            listings=[],
            errors=["Timeout while loading page"],
        )

        assert result.success is False
        assert len(result.listings) == 0
        assert "Timeout" in result.errors[0]


# ═══════════════════════════════════════════════════════════════════════════════
# CIRCUIT BREAKER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestCircuitBreaker:
    """Tests for circuit breaker functionality."""

    def test_circuit_states(self):
        """Test circuit breaker state transitions."""
        from services.collector.circuit_breaker import (
            FAILURE_THRESHOLD,
            RECOVERY_TIMEOUT_SECONDS,
            CircuitState,
        )

        assert CircuitState.CLOSED.value == "closed"
        assert CircuitState.OPEN.value == "open"
        assert CircuitState.HALF_OPEN.value == "half_open"
        assert FAILURE_THRESHOLD > 0
        assert RECOVERY_TIMEOUT_SECONDS > 0


# ═══════════════════════════════════════════════════════════════════════════════
# THROTTLER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestThrottler:
    """Tests for throttling functionality."""

    def test_source_configs_exist(self):
        """Source configs are defined."""
        from services.collector.throttle import SOURCE_CONFIGS

        assert "daft" in SOURCE_CONFIGS
        assert "rent" in SOURCE_CONFIGS
        assert SOURCE_CONFIGS["daft"]["base_delay"] > 0

    def test_random_delay_range(self):
        """Random delay is within expected range."""
        from services.collector.throttle import random_delay

        delay = random_delay(1.0, 2.0)
        assert 1.0 <= delay <= 2.0

    def test_add_jitter(self):
        """Jitter adds randomness to delay."""
        from services.collector.throttle import add_jitter

        base = 1.0
        jittered = add_jitter(base, 0.2)
        assert 0.8 <= jittered <= 1.2


# ═══════════════════════════════════════════════════════════════════════════════
# ANOMALY DETECTION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestAnomalyDetection:
    """Tests for anomaly detection."""

    def test_anomaly_types(self):
        """Anomaly types are defined."""
        from services.collector.anomaly_detection import AnomalyType

        assert AnomalyType.VOLUME_DROP.value == "volume_drop"
        assert AnomalyType.PARSE_FAILURE_SPIKE.value == "parse_failure_spike"
        assert AnomalyType.RESPONSE_TIME_SPIKE.value == "response_time_spike"

    def test_detector_thresholds(self):
        """Detector has configurable thresholds."""
        from services.collector.anomaly_detection import (
            PARSE_FAILURE_THRESHOLD,
            RESPONSE_TIME_MULTIPLIER,
            VOLUME_DROP_THRESHOLD,
        )

        assert 0 < VOLUME_DROP_THRESHOLD < 1  # e.g., 0.5 = 50% drop
        assert 0 < PARSE_FAILURE_THRESHOLD <= 1
        assert RESPONSE_TIME_MULTIPLIER > 1


# ═══════════════════════════════════════════════════════════════════════════════
# JSON-LD EXTRACTOR TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestJsonLdExtractor:
    """Tests for JSON-LD extraction."""

    def test_extract_from_html(self):
        """Extract JSON-LD from HTML."""
        from services.collector.jsonld_extractor import extract_jsonld

        html = '''
        <html>
        <head>
            <script type="application/ld+json">
            {"@type": "RealEstateListing", "name": "Test"}
            </script>
        </head>
        </html>
        '''

        items = extract_jsonld(html)
        assert len(items) == 1
        assert items[0]["@type"] == "RealEstateListing"

    def test_extract_empty_html(self):
        """Handle HTML without JSON-LD."""
        from services.collector.jsonld_extractor import extract_jsonld

        html = "<html><body>No JSON-LD here</body></html>"
        items = extract_jsonld(html)
        assert items == []

    def test_handle_invalid_json(self):
        """Handle invalid JSON gracefully."""
        from services.collector.jsonld_extractor import extract_jsonld

        html = '''
        <script type="application/ld+json">
        {invalid json}
        </script>
        '''

        items = extract_jsonld(html)
        assert items == []  # Should not raise
