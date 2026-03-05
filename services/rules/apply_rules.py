"""
AGPARS Apply Rules

Orchestrate exclusion checks and set appropriate flags.
"""

from typing import Any

from packages.observability.logger import get_logger
from packages.observability.metrics import get_metrics
from services.rules.exclusions import check_exclusion

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# APPLY RULES
# ═══════════════════════════════════════════════════════════════════════════════


def apply_rules(raw_listing: Any, normalized: Any) -> str | None:
    """
    Apply all rules to a listing.

    This is the main entry point for the rules engine.

    Args:
        raw_listing: Raw listing object
        normalized: Normalized listing object

    Returns:
        Exclusion reason if excluded, None otherwise
    """
    # Check exclusion rules
    exclusion = check_exclusion(raw_listing, normalized)

    if exclusion.is_excluded:
        # Log and increment metrics
        logger.info(
            "Listing excluded",
            rule=exclusion.rule,
            reason=exclusion.reason,
        )

        try:
            metrics = get_metrics()
            source = getattr(raw_listing, "source", "unknown") if raw_listing else "unknown"
            if hasattr(metrics, "increment"):
                metrics.increment(
                    "listings_excluded_total",
                    labels={
                        "reason": exclusion.rule or "unknown",
                        "source": source,
                    },
                )
        except Exception as e:
            logger.warning("Failed to increment exclusion metrics", error=str(e))

        return exclusion.reason

    # Set lease_length_unknown flag if needed
    if hasattr(normalized, "lease_length_months") and normalized.lease_length_months is None:
        normalized.lease_length_unknown = True

    return None


def batch_apply_rules(
    raw_listings: list[Any],
    normalized_listings: list[Any],
) -> tuple[list[Any], dict]:
    """
    Apply rules to a batch of listings.

    Args:
        raw_listings: List of raw listings
        normalized_listings: List of normalized listings

    Returns:
        Tuple of (filtered_listings, stats)
    """
    if len(raw_listings) != len(normalized_listings):
        raise ValueError("Raw and normalized listings must have same length")

    filtered = []
    stats = {
        "total": len(raw_listings),
        "passed": 0,
        "excluded": 0,
        "by_rule": {},
    }

    for raw, normalized in zip(raw_listings, normalized_listings, strict=False):
        reason = apply_rules(raw, normalized)

        if reason is None:
            filtered.append(normalized)
            stats["passed"] += 1
        else:
            stats["excluded"] += 1

            # Track by rule
            rule_key = reason.split()[0].lower()  # First word
            stats["by_rule"][rule_key] = stats["by_rule"].get(rule_key, 0) + 1

    logger.info("Batch rules applied", **stats)
    return filtered, stats


# ═══════════════════════════════════════════════════════════════════════════════
# POST-PROCESSING FLAGS
# ═══════════════════════════════════════════════════════════════════════════════


def set_lease_unknown_flag(normalized: Any) -> None:
    """Set the lease_length_unknown flag if lease info is missing."""
    if hasattr(normalized, "lease_length_months"):
        if normalized.lease_length_months is None:
            normalized.lease_length_unknown = True
        else:
            normalized.lease_length_unknown = False


def set_location_flags(normalized: Any) -> None:
    """Set location-related flags."""
    # If no city_id, mark as unlocated
    if hasattr(normalized, "city_id"):
        if normalized.city_id is None and hasattr(normalized, "is_located"):
            normalized.is_located = False
        elif normalized.city_id is not None:
            normalized.is_located = True


# ═══════════════════════════════════════════════════════════════════════════════
# RULE VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════


def validate_rules_config() -> list[str]:
    """
    Validate rules configuration.

    Returns:
        List of validation warnings
    """
    warnings = []

    # Check that exclusion patterns are loaded
    from services.rules.exclusions import NORTHERN_IRELAND_LOCATIONS, STUDENT_KEYWORDS

    if not STUDENT_KEYWORDS:
        warnings.append("No student housing keywords configured")

    if not NORTHERN_IRELAND_LOCATIONS:
        warnings.append("No Northern Ireland locations configured")

    return warnings


def get_rules_summary() -> dict:
    """Get summary of active rules."""
    from services.rules.exclusions import (
        NORTHERN_IRELAND_LOCATIONS,
        SHORT_TERM_KEYWORDS,
        STUDENT_KEYWORDS,
    )

    return {
        "exclusion_rules": {
            "student_housing": {
                "enabled": True,
                "keywords_count": len(STUDENT_KEYWORDS),
            },
            "short_term": {
                "enabled": True,
                "keywords_count": len(SHORT_TERM_KEYWORDS),
                "min_lease_months": 6,
            },
            "northern_ireland": {
                "enabled": True,
                "locations_count": len(NORTHERN_IRELAND_LOCATIONS),
            },
        },
        "flags": ["lease_length_unknown", "is_located"],
    }
