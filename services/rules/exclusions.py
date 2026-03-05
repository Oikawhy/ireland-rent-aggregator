"""
AGPARS Exclusion Rules Engine

Apply hard exclusions for student housing, short-term rentals, Northern Ireland.
"""

import re
from dataclasses import dataclass
from typing import Any

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCLUSION PATTERNS
# ═══════════════════════════════════════════════════════════════════════════════


# Student housing keywords
STUDENT_KEYWORDS = [
    "student accommodation",
    "student housing",
    "student only",
    "students only",
    "student let",
    "student rental",
    "student rooms",
    "student living",
    "university accommodation",
    "college accommodation",
    "campus accommodation",
    "digs",  # Irish slang for student rooms
    "purpose built student",
    "pbsa",  # Purpose Built Student Accommodation
]

# Short-term rental keywords (< 6 months)
SHORT_TERM_KEYWORDS = [
    "short term",
    "short-term",
    "shortterm",
    "holiday let",
    "holiday rental",
    "vacation rental",
    "weekly rental",
    "monthly rental",  # only if < 6 months specified
    "airbnb",
    "corporate let",  # often short-term
    "temporary accommodation",
]

# Short-term lease length patterns
SHORT_TERM_PATTERNS = [
    r"\b1\s*month\b",
    r"\bweekly\b",
    r"\bper\s*week\b",
    r"\b(\d+)\s*nights?\b",
    r"\bshort\s*stay\b",
]

# Northern Ireland locations
NORTHERN_IRELAND_LOCATIONS = [
    "antrim",
    "armagh",
    "belfast",
    "derry",
    "londonderry",
    "down",
    "fermanagh",
    "tyrone",
    "newry",
    "lisburn",
    "bangor",  # NI Bangor, not Wales
    "carrickfergus",
    "coleraine",
    "ballymena",
    "newtownabbey",
    "portadown",
    "lurgan",
    "omagh",
    "enniskillen",
    "strabane",
    "cookstown",
    "northern ireland",
    "county antrim",
    "county armagh",
    "county down",
    "county fermanagh",
    "county tyrone",
    "co. antrim",
    "co. armagh",
    "co. down",
    "co. fermanagh",
    "co. tyrone",
]


# ═══════════════════════════════════════════════════════════════════════════════
# EXCLUSION RESULTS
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class ExclusionResult:
    """Result of exclusion check."""

    is_excluded: bool
    reason: str | None
    rule: str | None


# ═══════════════════════════════════════════════════════════════════════════════
# EXCLUSION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════


class ExclusionEngine:
    """
    Apply hard exclusion rules.

    Exclusions:
    1. Student housing
    2. Short-term rentals (< 6 months)
    3. Northern Ireland locations
    """

    def __init__(self):
        self.logger = get_logger(__name__)

        # Pre-compile patterns
        self._student_pattern = self._compile_keywords(STUDENT_KEYWORDS)
        self._short_term_pattern = self._compile_keywords(SHORT_TERM_KEYWORDS)
        self._ni_pattern = self._compile_keywords(NORTHERN_IRELAND_LOCATIONS)
        self._short_term_lease_patterns = [
            re.compile(p, re.IGNORECASE) for p in SHORT_TERM_PATTERNS
        ]

    def _compile_keywords(self, keywords: list[str]) -> re.Pattern:
        """Compile keywords into a single regex pattern."""
        escaped = [re.escape(k) for k in keywords]
        pattern = r"\b(" + "|".join(escaped) + r")\b"
        return re.compile(pattern, re.IGNORECASE)

    def check_exclusion(self, raw_listing: Any, normalized: Any = None) -> ExclusionResult:
        """
        Check if a listing should be excluded.

        Args:
            raw_listing: Raw listing data
            normalized: Optional normalized listing data

        Returns:
            ExclusionResult with exclusion status and reason
        """
        # Gather text to check
        texts = self._gather_texts(raw_listing, normalized)
        combined_text = " ".join(texts)

        # Check student housing
        if self._is_student_housing(combined_text):
            return ExclusionResult(
                is_excluded=True,
                reason="Student accommodation",
                rule="student_housing",
            )

        # Check short-term
        if self._is_short_term(combined_text, normalized):
            return ExclusionResult(
                is_excluded=True,
                reason="Short-term rental (< 6 months)",
                rule="short_term",
            )

        # Check Northern Ireland
        location_text = getattr(raw_listing, "location_text", "") or ""
        if normalized:
            location_text += " " + (getattr(normalized, "area_text", "") or "")
            location_text += " " + (getattr(normalized, "county", "") or "")

        if self._is_northern_ireland(location_text):
            return ExclusionResult(
                is_excluded=True,
                reason="Northern Ireland location",
                rule="northern_ireland",
            )

        return ExclusionResult(is_excluded=False, reason=None, rule=None)

    def _gather_texts(self, raw_listing: Any, _normalized: Any = None) -> list[str]:
        """Gather all text fields for checking."""
        texts = []

        # From raw listing
        if hasattr(raw_listing, "title") and raw_listing.title:
            texts.append(raw_listing.title)
        if hasattr(raw_listing, "description") and raw_listing.description:
            texts.append(raw_listing.description)
        if hasattr(raw_listing, "location_text") and raw_listing.location_text:
            texts.append(raw_listing.location_text)

        # From raw_payload
        if hasattr(raw_listing, "raw_payload") and raw_listing.raw_payload:
            payload = raw_listing.raw_payload
            for key in ["title", "description", "features", "details"]:
                if key in payload and payload[key]:
                    texts.append(str(payload[key]))

        return texts

    def _is_student_housing(self, text: str) -> bool:
        """Check for student housing indicators."""
        return bool(self._student_pattern.search(text))

    def _is_short_term(self, text: str, normalized: Any = None) -> bool:
        """Check for short-term rental indicators."""
        # Keyword check
        if self._short_term_pattern.search(text):
            return True

        # Lease pattern check
        for pattern in self._short_term_lease_patterns:
            if pattern.search(text):
                return True

        # Check explicit lease length (< 6 months is short-term)
        if normalized:
            lease_months = getattr(normalized, "lease_length_months", None)
            if lease_months is not None and lease_months < 6:
                return True

        return False

    def _is_northern_ireland(self, location_text: str) -> bool:
        """Check if location is in Northern Ireland."""
        return bool(self._ni_pattern.search(location_text))


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


_engine: ExclusionEngine | None = None


def get_engine() -> ExclusionEngine:
    """Get global exclusion engine instance."""
    global _engine
    if _engine is None:
        _engine = ExclusionEngine()
    return _engine


def is_excluded(raw_listing: Any, normalized: Any = None) -> bool:
    """Check if listing should be excluded."""
    result = get_engine().check_exclusion(raw_listing, normalized)
    return result.is_excluded


def get_exclusion_reason(raw_listing: Any, normalized: Any = None) -> str | None:
    """Get exclusion reason if excluded."""
    result = get_engine().check_exclusion(raw_listing, normalized)
    return result.reason if result.is_excluded else None


def check_exclusion(raw_listing: Any, normalized: Any = None) -> ExclusionResult:
    """Full exclusion check returning result object."""
    return get_engine().check_exclusion(raw_listing, normalized)
