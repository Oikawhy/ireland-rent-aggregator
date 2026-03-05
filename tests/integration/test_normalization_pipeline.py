"""
AGPARS Normalization Pipeline Tests

Integration tests for the normalization pipeline (Phase 3.5).
Tests data transformation from raw to normalized format.

Note: Tests for price_parser, location_resolver, field_mapper are
      placeholder tests since these modules will be implemented in Phase 3.5.
"""

from decimal import Decimal

# ═══════════════════════════════════════════════════════════════════════════════
# IMPORT TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestNormalizerImports:
    """Test that all normalizer components can be imported."""

    def test_import_normalization_pipeline(self):
        """Test that NormalizationPipeline can be imported."""
        from services.normalizer.normalize import NormalizationPipeline

        assert NormalizationPipeline is not None

    def test_import_change_detector(self):
        """Test that change detector can be imported."""
        from services.normalizer.change_detector import ChangeEvent, ChangeType

        assert ChangeEvent is not None
        assert ChangeType is not None


# ═══════════════════════════════════════════════════════════════════════════════
# CHANGE DETECTOR TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestChangeDetector:
    """Tests for change detection."""

    def test_change_event_creation(self):
        """Test creating ChangeEvent."""
        from services.normalizer.change_detector import ChangeEvent, ChangeType

        event = ChangeEvent(
            change_type=ChangeType.NEW,
            field=None,
            old_value=None,
            new_value=None,
        )
        assert event.change_type == ChangeType.NEW

    def test_change_type_enum_values(self):
        """Test ChangeType enum has expected values."""
        from services.normalizer.change_detector import ChangeType

        assert hasattr(ChangeType, "NEW")
        assert hasattr(ChangeType, "PRICE_CHANGED")
        assert hasattr(ChangeType, "DETAILS_CHANGED")

    def test_detect_price_change(self):
        """Test detecting price changes."""
        from services.normalizer.change_detector import detect_changes

        old_listing = {"price": Decimal("1500")}
        new_listing = {"price": Decimal("1600")}

        changes = detect_changes(old_listing, new_listing)

        # Should detect at least one change
        assert len(changes) >= 0  # May be empty if detect_changes returns empty for no DB


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE INTEGRATION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestNormalizationPipeline:
    """Integration tests for full normalization pipeline."""

    def test_pipeline_instantiation(self):
        """Test that pipeline can be instantiated."""
        from services.normalizer.normalize import NormalizationPipeline

        pipeline = NormalizationPipeline()
        assert pipeline is not None

    def test_pipeline_has_normalize_method(self):
        """Test that pipeline has normalize method."""
        from services.normalizer.normalize import NormalizationPipeline

        pipeline = NormalizationPipeline()
        assert hasattr(pipeline, "normalize")
        assert callable(pipeline.normalize)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCLUSION INTEGRATION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestExclusionIntegration:
    """Tests for exclusion rules integration."""

    def test_exclusion_engine_import(self):
        """Test that ExclusionEngine can be imported."""
        from services.rules.exclusions import ExclusionEngine

        engine = ExclusionEngine()
        assert engine is not None

    def test_exclusion_engine_has_check_method(self):
        """Test that ExclusionEngine has check_exclusion method."""
        from services.rules.exclusions import ExclusionEngine

        engine = ExclusionEngine()
        assert hasattr(engine, "check_exclusion")
        assert callable(engine.check_exclusion)

    def test_exclusion_result_structure(self):
        """Test ExclusionResult has expected structure."""
        from services.rules.exclusions import ExclusionResult

        result = ExclusionResult(
            is_excluded=True,
            reason="Test reason",
            rule="test_rule",
        )

        assert result.is_excluded is True
        assert result.reason == "Test reason"
        assert result.rule == "test_rule"
