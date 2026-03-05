"""
Tests for Normalization Pipeline

T065.5 - Tests for services/normalizer/
"""

from decimal import Decimal

# ═══════════════════════════════════════════════════════════════════════════════
# CITY SYNONYMS TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestCitySynonyms:
    """Tests for city synonym resolution."""

    def test_dublin_postal_code(self):
        """Resolve Dublin postal codes."""
        from services.normalizer.city_synonyms import _resolve_dublin_postal

        assert _resolve_dublin_postal("Dublin 4") == "Dublin 4"
        assert _resolve_dublin_postal("D4") == "Dublin 4"
        assert _resolve_dublin_postal("dublin4") == "Dublin 4"

    def test_dublin_15(self):
        """Resolve Dublin 15."""
        from services.normalizer.city_synonyms import _resolve_dublin_postal

        assert _resolve_dublin_postal("Dublin 15") == "Dublin 15"
        assert _resolve_dublin_postal("D15") == "Dublin 15"

    def test_dublin_6w(self):
        """Resolve Dublin 6W."""
        from services.normalizer.city_synonyms import _resolve_dublin_postal

        assert _resolve_dublin_postal("Dublin 6W") == "Dublin 6W"
        assert _resolve_dublin_postal("D6W") == "Dublin 6W"

    def test_city_synonyms(self):
        """Resolve city synonyms."""
        from services.normalizer.city_synonyms import _resolve_synonym

        assert _resolve_synonym("dublin city centre") == "Dublin"
        assert _resolve_synonym("cork city") == "Cork"
        assert _resolve_synonym("galway city centre") == "Galway"

    def test_case_insensitive(self):
        """Synonym resolution is case-insensitive."""
        from services.normalizer.city_synonyms import _resolve_synonym

        assert _resolve_synonym("DUBLIN CITY CENTRE") == "Dublin"


# ═══════════════════════════════════════════════════════════════════════════════
# COUNTY FILTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestCountyFilter:
    """Tests for county filtering."""

    def test_detect_dublin(self):
        """Detect Dublin county."""
        from services.normalizer.county_filter import detect_county

        assert detect_county("Dublin") == "Dublin"
        assert detect_county("County Dublin") == "Dublin"
        assert detect_county("Co. Dublin") == "Dublin"

    def test_detect_cork(self):
        """Detect Cork county."""
        from services.normalizer.county_filter import detect_county

        assert detect_county("Cork") == "Cork"
        assert detect_county("County Cork") == "Cork"

    def test_detect_galway(self):
        """Detect Galway county."""
        from services.normalizer.county_filter import detect_county

        assert detect_county("Galway City, County Galway") == "Galway"

    def test_detect_none(self):
        """Return None for unknown location."""
        from services.normalizer.county_filter import detect_county

        assert detect_county("Unknown Place") is None
        assert detect_county("") is None
        assert detect_county(None) is None


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATOR TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestValidators:
    """Tests for data validators."""

    def test_valid_price(self):
        """Valid price passes validation."""
        from services.normalizer.validators import validate_price

        assert validate_price(1500) == []
        assert validate_price(Decimal("2500.00")) == []

    def test_price_too_low(self):
        """Price too low fails validation."""
        from services.normalizer.validators import validate_price

        errors = validate_price(50)
        assert len(errors) == 1
        assert "too low" in errors[0].lower()

    def test_price_too_high(self):
        """Price too high fails validation."""
        from services.normalizer.validators import validate_price

        errors = validate_price(100000)
        assert len(errors) == 1
        assert "too high" in errors[0].lower()

    def test_valid_beds(self):
        """Valid bed count passes."""
        from services.normalizer.validators import validate_beds

        assert validate_beds(2) == []
        assert validate_beds(0) == []  # Studio
        assert validate_beds(5) == []

    def test_invalid_beds(self):
        """Invalid bed count fails."""
        from services.normalizer.validators import validate_beds

        errors = validate_beds(100)
        assert len(errors) == 1

    def test_valid_listing(self):
        """Valid listing passes all validations."""
        from services.normalizer.validators import is_valid_listing, validate_listing

        listing = {
            "price": 1500,
            "beds": 2,
            "baths": 1,
            "city_id": 123,
        }

        assert validate_listing(listing) == []
        assert is_valid_listing(listing)


# ═══════════════════════════════════════════════════════════════════════════════
# CHANGE DETECTOR INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════


class TestChangeDetectorIntegration:
    """Integration tests for change detection."""

    def test_detect_new_and_format(self):
        """Detect new listing and format message."""
        from services.normalizer.change_detector import (
            ChangeType,
            detect_changes,
            format_change_event,
        )

        new = {"id": 1, "price": 1500}
        changes = detect_changes(new, None)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.NEW

        formatted = format_change_event(changes[0])
        assert "New" in formatted

    def test_price_change_flow(self):
        """Full price change detection flow."""
        from services.normalizer.change_detector import (
            ChangeType,
            detect_changes,
            has_significant_changes,
        )

        old = {"id": 1, "price": 1000, "beds": 2}
        new = {"id": 1, "price": 1200, "beds": 2}  # 20% increase

        assert has_significant_changes(new, old)

        changes = detect_changes(new, old)
        price_changes = [c for c in changes if c.change_type == ChangeType.PRICE_CHANGED]
        assert len(price_changes) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# NORMALIZATION PIPELINE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestNormalizationPipeline:
    """Tests for the full normalization pipeline."""

    def test_pipeline_initialization(self):
        """Pipeline can be initialized."""
        from services.normalizer.normalize import NormalizationPipeline

        pipeline = NormalizationPipeline()
        assert pipeline is not None

    def test_extract_price_from_text(self):
        """Extract price from text."""
        from services.collector.sanitize import sanitize_price

        assert sanitize_price("€1,500 per month") == Decimal("1500")
        assert sanitize_price("€2,000/month") == Decimal("2000")
        assert sanitize_price("1500") == Decimal("1500")

    def test_extract_beds_from_text(self):
        """Extract beds from text."""
        from services.collector.sanitize import sanitize_beds

        assert sanitize_beds("2") == 2
        assert sanitize_beds("2 beds") == 2
        assert sanitize_beds("Studio") == 0
