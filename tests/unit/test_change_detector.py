"""
Tests for Change Detector

T030 - Tests for services/normalizer/change_detector.py
"""

from decimal import Decimal

from services.normalizer.change_detector import (
    ChangeDetector,
    ChangeEvent,
    ChangeType,
    detect_changes,
    format_change_event,
    has_significant_changes,
)

# ═══════════════════════════════════════════════════════════════════════════════
# NEW LISTING TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestNewListingDetection:
    """Tests for detecting new listings."""

    def test_new_listing_none_existing(self):
        """New listing when existing is None."""
        new = {"id": 1, "price": 1500, "beds": 2}
        changes = detect_changes(new, None)

        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.NEW

    def test_new_listing_is_significant(self):
        """New listings are always significant."""
        new = {"id": 1, "price": 1500}
        assert has_significant_changes(new, None)


# ═══════════════════════════════════════════════════════════════════════════════
# PRICE CHANGE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestPriceChangeDetection:
    """Tests for price change detection."""

    def test_significant_price_increase(self):
        """Detect significant price increase (>5%)."""
        old = {"price": 1000}
        new = {"price": 1100}  # 10% increase

        detector = ChangeDetector()
        changes = detector.detect_changes(new, old)

        price_changes = [c for c in changes if c.change_type == ChangeType.PRICE_CHANGED]
        assert len(price_changes) == 1
        assert price_changes[0].old_value == 1000
        assert price_changes[0].new_value == 1100

    def test_significant_price_decrease(self):
        """Detect significant price decrease (>5%)."""
        old = {"price": 2000}
        new = {"price": 1800}  # 10% decrease

        changes = detect_changes(new, old)
        price_changes = [c for c in changes if c.change_type == ChangeType.PRICE_CHANGED]
        assert len(price_changes) == 1

    def test_insignificant_price_change(self):
        """No event for small price changes (<5%)."""
        old = {"price": 1000}
        new = {"price": 1020}  # 2% change

        changes = detect_changes(new, old)
        price_changes = [c for c in changes if c.change_type == ChangeType.PRICE_CHANGED]
        assert len(price_changes) == 0

    def test_price_threshold_exactly_5_percent(self):
        """Price change at exactly 5% is significant."""
        old = {"price": 1000}
        new = {"price": 1050}  # exactly 5%

        changes = detect_changes(new, old)
        price_changes = [c for c in changes if c.change_type == ChangeType.PRICE_CHANGED]
        assert len(price_changes) == 1

    def test_price_change_with_decimal(self):
        """Handle Decimal price values."""
        old = {"price": Decimal("1500.00")}
        new = {"price": Decimal("1800.00")}

        changes = detect_changes(new, old)
        price_changes = [c for c in changes if c.change_type == ChangeType.PRICE_CHANGED]
        assert len(price_changes) == 1

    def test_price_from_none(self):
        """Price change from None."""
        old = {"price": None, "beds": 2}
        new = {"price": 1500, "beds": 2}

        changes = detect_changes(new, old)
        price_changes = [c for c in changes if c.change_type == ChangeType.PRICE_CHANGED]
        assert len(price_changes) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# BEDS/BATHS CHANGE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestBedsChangeDetection:
    """Tests for bedroom count changes."""

    def test_beds_changed(self):
        """Detect bedroom count change."""
        old = {"beds": 2}
        new = {"beds": 3}

        changes = detect_changes(new, old)
        beds_changes = [c for c in changes if c.change_type == ChangeType.BEDS_CHANGED]
        assert len(beds_changes) == 1
        assert beds_changes[0].old_value == 2
        assert beds_changes[0].new_value == 3

    def test_beds_unchanged(self):
        """No change when beds same."""
        old = {"beds": 2}
        new = {"beds": 2}

        changes = detect_changes(new, old)
        beds_changes = [c for c in changes if c.change_type == ChangeType.BEDS_CHANGED]
        assert len(beds_changes) == 0

    def test_baths_changed(self):
        """Detect bathroom count change."""
        old = {"baths": 1}
        new = {"baths": 2}

        changes = detect_changes(new, old)
        baths_changes = [c for c in changes if c.field == "baths"]
        assert len(baths_changes) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# STATUS CHANGE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestStatusChangeDetection:
    """Tests for listing status changes."""

    def test_removed_status(self):
        """Detect removed status."""
        old = {"status": "active"}
        new = {"status": "removed"}

        changes = detect_changes(new, old)
        status_changes = [c for c in changes if c.change_type == ChangeType.REMOVED]
        assert len(status_changes) == 1

    def test_reactivated_status(self):
        """Detect reactivated listing."""
        old = {"status": "removed"}
        new = {"status": "active"}

        changes = detect_changes(new, old)
        status_changes = [c for c in changes if c.change_type == ChangeType.REACTIVATED]
        assert len(status_changes) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# MULTIPLE CHANGES TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestMultipleChanges:
    """Tests for multiple simultaneous changes."""

    def test_multiple_changes(self):
        """Detect multiple changes at once."""
        old = {"price": 1000, "beds": 2, "baths": 1}
        new = {"price": 1200, "beds": 3, "baths": 2}

        changes = detect_changes(new, old)
        assert len(changes) >= 2  # Price + beds at minimum


# ═══════════════════════════════════════════════════════════════════════════════
# FORMAT CHANGE EVENT TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestFormatChangeEvent:
    """Tests for change event formatting."""

    def test_format_new(self):
        """Format new listing event."""
        event = ChangeEvent(
            change_type=ChangeType.NEW,
            field=None,
            old_value=None,
            new_value=None,
        )
        formatted = format_change_event(event)
        assert "New listing" in formatted

    def test_format_price_changed(self):
        """Format price change event."""
        event = ChangeEvent(
            change_type=ChangeType.PRICE_CHANGED,
            field="price",
            old_value=1000,
            new_value=1200,
        )
        formatted = format_change_event(event)
        assert "1000" in formatted
        assert "1200" in formatted

    def test_format_removed(self):
        """Format removed event."""
        event = ChangeEvent(
            change_type=ChangeType.REMOVED,
            field="status",
            old_value="active",
            new_value="removed",
        )
        formatted = format_change_event(event)
        assert "removed" in formatted.lower()
