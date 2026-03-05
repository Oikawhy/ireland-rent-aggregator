"""
AGPARS Change Detector

Detect significant changes between listing versions.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT TYPES
# ═══════════════════════════════════════════════════════════════════════════════


class ChangeType(Enum):
    """Types of listing changes."""

    NEW = "new"
    PRICE_CHANGED = "price_changed"
    BEDS_CHANGED = "beds_changed"
    STATUS_CHANGED = "status_changed"
    DETAILS_CHANGED = "details_changed"
    REMOVED = "removed"
    REACTIVATED = "reactivated"


@dataclass
class ChangeEvent:
    """Detected change between listing versions."""

    change_type: ChangeType
    field: str | None
    old_value: Any
    new_value: Any
    is_significant: bool = True


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNIFICANCE THRESHOLDS
# ═══════════════════════════════════════════════════════════════════════════════


# Price change threshold (percentage)
PRICE_CHANGE_THRESHOLD_PERCENT = 5.0

# Fields that are considered significant
SIGNIFICANT_FIELDS = {"price", "beds", "baths", "status", "property_type"}


# ═══════════════════════════════════════════════════════════════════════════════
# CHANGE DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════


class ChangeDetector:
    """
    Detects changes between old and new listing versions.

    Significant changes:
    - Price change > 5%
    - Bedroom/bathroom count
    - Status (active/removed)
    - Property type
    """

    def __init__(self, price_threshold_percent: float = PRICE_CHANGE_THRESHOLD_PERCENT):
        self.price_threshold = price_threshold_percent

    def detect_changes(
        self,
        new_listing: dict,
        existing_listing: dict | None,
    ) -> list[ChangeEvent]:
        """
        Detect all changes between versions.

        Args:
            new_listing: New listing data
            existing_listing: Existing listing data (None if new)

        Returns:
            List of ChangeEvent objects
        """
        if existing_listing is None:
            return [ChangeEvent(
                change_type=ChangeType.NEW,
                field=None,
                old_value=None,
                new_value=None,
            )]

        changes = []

        # Check price
        price_change = self._detect_price_change(new_listing, existing_listing)
        if price_change:
            changes.append(price_change)

        # Check beds
        if new_listing.get("beds") != existing_listing.get("beds"):
            changes.append(ChangeEvent(
                change_type=ChangeType.BEDS_CHANGED,
                field="beds",
                old_value=existing_listing.get("beds"),
                new_value=new_listing.get("beds"),
            ))

        # Check baths
        if new_listing.get("baths") != existing_listing.get("baths"):
            changes.append(ChangeEvent(
                change_type=ChangeType.DETAILS_CHANGED,
                field="baths",
                old_value=existing_listing.get("baths"),
                new_value=new_listing.get("baths"),
            ))

        # Check status
        if new_listing.get("status") != existing_listing.get("status"):
            old_status = existing_listing.get("status")
            new_status = new_listing.get("status")

            if new_status == "removed":
                change_type = ChangeType.REMOVED
            elif old_status == "removed" and new_status == "active":
                change_type = ChangeType.REACTIVATED
            else:
                change_type = ChangeType.STATUS_CHANGED

            changes.append(ChangeEvent(
                change_type=change_type,
                field="status",
                old_value=old_status,
                new_value=new_status,
            ))

        # Check property type
        if new_listing.get("property_type") != existing_listing.get("property_type"):
            changes.append(ChangeEvent(
                change_type=ChangeType.DETAILS_CHANGED,
                field="property_type",
                old_value=existing_listing.get("property_type"),
                new_value=new_listing.get("property_type"),
            ))

        # Check furnished
        if new_listing.get("furnished") != existing_listing.get("furnished"):
            changes.append(ChangeEvent(
                change_type=ChangeType.DETAILS_CHANGED,
                field="furnished",
                old_value=existing_listing.get("furnished"),
                new_value=new_listing.get("furnished"),
                is_significant=False,
            ))

        return changes

    def _detect_price_change(
        self,
        new_listing: dict,
        existing_listing: dict,
    ) -> ChangeEvent | None:
        """Detect significant price change."""
        old_price = existing_listing.get("price")
        new_price = new_listing.get("price")

        if old_price is None or new_price is None:
            if old_price != new_price:
                return ChangeEvent(
                    change_type=ChangeType.PRICE_CHANGED,
                    field="price",
                    old_value=old_price,
                    new_value=new_price,
                )
            return None

        # Convert to float for comparison
        try:
            old_float = float(old_price)
            new_float = float(new_price)
        except (TypeError, ValueError):
            return None

        if old_float == 0:
            return None

        # Calculate percentage change
        percent_change = abs(new_float - old_float) / old_float * 100

        if percent_change >= self.price_threshold:
            return ChangeEvent(
                change_type=ChangeType.PRICE_CHANGED,
                field="price",
                old_value=old_price,
                new_value=new_price,
            )

        return None

    def has_significant_changes(
        self,
        new_listing: dict,
        existing_listing: dict | None,
    ) -> bool:
        """Check if there are any significant changes."""
        changes = self.detect_changes(new_listing, existing_listing)
        return any(c.is_significant for c in changes)


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def detect_changes(new_listing: dict, existing_listing: dict | None) -> list[ChangeEvent]:
    """Convenience function for change detection."""
    detector = ChangeDetector()
    return detector.detect_changes(new_listing, existing_listing)


def has_significant_changes(new_listing: dict, existing_listing: dict | None) -> bool:
    """Check if there are significant changes."""
    detector = ChangeDetector()
    return detector.has_significant_changes(new_listing, existing_listing)


def format_change_event(event: ChangeEvent) -> str:
    """Format a change event for display."""
    if event.change_type == ChangeType.NEW:
        return "New listing"
    elif event.change_type == ChangeType.PRICE_CHANGED:
        return f"Price changed: {event.old_value} → {event.new_value}"
    elif event.change_type == ChangeType.REMOVED:
        return "Listing removed"
    elif event.change_type == ChangeType.REACTIVATED:
        return "Listing reactivated"
    else:
        return f"{event.field} changed: {event.old_value} → {event.new_value}"
