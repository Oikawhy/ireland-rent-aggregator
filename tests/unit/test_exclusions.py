"""
Tests for Exclusion Rules

T033 - Tests for services/rules/exclusions.py
"""

from dataclasses import dataclass

from services.rules.exclusions import (
    ExclusionResult,
    check_exclusion,
    get_exclusion_reason,
    is_excluded,
)

# ═══════════════════════════════════════════════════════════════════════════════
# TEST DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class MockRawListing:
    """Mock raw listing for testing."""

    title: str | None = None
    description: str | None = None
    location_text: str | None = None
    raw_payload: dict | None = None


@dataclass
class MockNormalizedListing:
    """Mock normalized listing for testing."""

    area_text: str | None = None
    county: str | None = None
    lease_length_months: int | None = None


# ═══════════════════════════════════════════════════════════════════════════════
# STUDENT HOUSING EXCLUSION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestStudentHousingExclusion:
    """Tests for student housing exclusion."""

    def test_student_accommodation_in_title(self):
        """Exclude 'student accommodation' in title."""
        raw = MockRawListing(title="Student Accommodation near UCD")
        assert is_excluded(raw)
        assert "Student" in get_exclusion_reason(raw)

    def test_student_only_in_description(self):
        """Exclude 'students only' in description."""
        raw = MockRawListing(description="This property is for students only")
        assert is_excluded(raw)

    def test_digs_slang(self):
        """Exclude 'digs' (Irish slang for student rooms)."""
        raw = MockRawListing(title="Digs available near Trinity")
        assert is_excluded(raw)

    def test_pbsa(self):
        """Exclude PBSA (Purpose Built Student Accommodation)."""
        raw = MockRawListing(title="Modern PBSA in Dublin City")
        assert is_excluded(raw)

    def test_university_accommodation(self):
        """Exclude university accommodation."""
        raw = MockRawListing(description="University accommodation available")
        assert is_excluded(raw)

    def test_normal_listing_not_excluded(self):
        """Normal listing without student keywords not excluded."""
        raw = MockRawListing(title="2 Bed Apartment in Dublin")
        assert not is_excluded(raw)

    def test_case_insensitive_student(self):
        """Student exclusion is case-insensitive."""
        raw = MockRawListing(title="STUDENT HOUSING AVAILABLE")
        assert is_excluded(raw)


# ═══════════════════════════════════════════════════════════════════════════════
# SHORT-TERM RENTAL EXCLUSION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestShortTermExclusion:
    """Tests for short-term rental exclusion."""

    def test_short_term_in_title(self):
        """Exclude 'short term' in title."""
        raw = MockRawListing(title="Short term rental available")
        assert is_excluded(raw)
        assert "Short-term" in get_exclusion_reason(raw)

    def test_holiday_let(self):
        """Exclude holiday lets."""
        raw = MockRawListing(title="Holiday let in Galway")
        assert is_excluded(raw)

    def test_airbnb(self):
        """Exclude AirBnB rentals."""
        raw = MockRawListing(description="Also listed on Airbnb")
        assert is_excluded(raw)

    def test_weekly_rental(self):
        """Exclude weekly rentals."""
        raw = MockRawListing(title="Weekly rental, €300 per week")
        assert is_excluded(raw)

    def test_1_month_lease(self):
        """Exclude 1 month lease."""
        raw = MockRawListing(description="Available for 1 month only")
        assert is_excluded(raw)

    def test_explicit_short_lease(self):
        """Exclude if normalized lease < 6 months."""
        raw = MockRawListing(title="Apartment in Dublin")
        normalized = MockNormalizedListing(lease_length_months=1)
        assert is_excluded(raw, normalized)

    def test_explicit_3_month_lease_excluded(self):
        """Exclude 3 month lease (< 6 months)."""
        raw = MockRawListing(title="Apartment in Dublin")
        normalized = MockNormalizedListing(lease_length_months=3)
        assert is_excluded(raw, normalized)
        assert "6 months" in get_exclusion_reason(raw, normalized)

    def test_explicit_5_month_lease_excluded(self):
        """Exclude 5 month lease (< 6 months)."""
        raw = MockRawListing(title="Apartment in Dublin")
        normalized = MockNormalizedListing(lease_length_months=5)
        assert is_excluded(raw, normalized)

    def test_explicit_6_month_lease_not_excluded(self):
        """6 month lease is NOT excluded (>= 6 months)."""
        raw = MockRawListing(title="Apartment in Dublin")
        normalized = MockNormalizedListing(lease_length_months=6)
        assert not is_excluded(raw, normalized)

    def test_explicit_12_month_lease_not_excluded(self):
        """12 month lease is NOT excluded."""
        raw = MockRawListing(title="Apartment in Dublin")
        normalized = MockNormalizedListing(lease_length_months=12)
        assert not is_excluded(raw, normalized)

    def test_long_term_not_excluded(self):
        """Long-term (12+ months) not excluded."""
        raw = MockRawListing(
            title="Long term rental available",
            description="Minimum 12 months lease"
        )
        # "Long term" is not a short-term keyword
        assert not is_excluded(raw)


# ═══════════════════════════════════════════════════════════════════════════════
# NORTHERN IRELAND EXCLUSION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestNorthernIrelandExclusion:
    """Tests for Northern Ireland exclusion."""

    def test_belfast_excluded(self):
        """Exclude Belfast listings."""
        raw = MockRawListing(location_text="Belfast, Northern Ireland")
        assert is_excluded(raw)
        assert "Northern Ireland" in get_exclusion_reason(raw)

    def test_derry_excluded(self):
        """Exclude Derry/Londonderry listings."""
        raw = MockRawListing(location_text="Derry")
        assert is_excluded(raw)

    def test_county_antrim_excluded(self):
        """Exclude County Antrim listings."""
        raw = MockRawListing(location_text="Ballymena, County Antrim")
        assert is_excluded(raw)

    def test_co_down_excluded(self):
        """Exclude Co. Down listings."""
        raw = MockRawListing(location_text="Bangor, Co. Down")
        assert is_excluded(raw)

    def test_armagh_excluded(self):
        """Exclude Armagh listings."""
        raw = MockRawListing(location_text="Armagh City")
        assert is_excluded(raw)

    def test_northern_ireland_explicit(self):
        """Exclude explicit Northern Ireland."""
        raw = MockRawListing(location_text="Somewhere in Northern Ireland")
        assert is_excluded(raw)

    def test_dublin_not_excluded(self):
        """Dublin (Republic) not excluded."""
        raw = MockRawListing(location_text="Dublin, Ireland")
        assert not is_excluded(raw)

    def test_cork_not_excluded(self):
        """Cork not excluded."""
        raw = MockRawListing(location_text="Cork City")
        assert not is_excluded(raw)

    def test_galway_not_excluded(self):
        """Galway not excluded."""
        raw = MockRawListing(location_text="Galway, County Galway")
        assert not is_excluded(raw)

    def test_ni_location_in_normalized(self):
        """Exclude NI from normalized area_text."""
        raw = MockRawListing(title="Nice apartment")
        normalized = MockNormalizedListing(area_text="Belfast, BT1")
        assert is_excluded(raw, normalized)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCLUSION RESULT TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestExclusionResult:
    """Tests for ExclusionResult structure."""

    def test_excluded_result_structure(self):
        """Excluded result has correct structure."""
        raw = MockRawListing(title="Student rooms available")
        result = check_exclusion(raw)

        assert isinstance(result, ExclusionResult)
        assert result.is_excluded is True
        assert result.reason is not None
        assert result.rule == "student_housing"

    def test_not_excluded_result_structure(self):
        """Non-excluded result has correct structure."""
        raw = MockRawListing(title="2 Bed Apartment Dublin")
        result = check_exclusion(raw)

        assert isinstance(result, ExclusionResult)
        assert result.is_excluded is False
        assert result.reason is None
        assert result.rule is None


# ═══════════════════════════════════════════════════════════════════════════════
# EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """Edge case tests."""

    def test_empty_listing(self):
        """Empty listing not excluded."""
        raw = MockRawListing()
        assert not is_excluded(raw)

    def test_none_values(self):
        """None values handled gracefully."""
        raw = MockRawListing(title=None, description=None, location_text=None)
        assert not is_excluded(raw)

    def test_raw_payload_checked(self):
        """Raw payload fields are checked."""
        raw = MockRawListing(
            title="Nice apartment",
            raw_payload={"description": "Student accommodation only"}
        )
        assert is_excluded(raw)

    def test_multiple_exclusion_reasons(self):
        """First matching rule is returned."""
        raw = MockRawListing(
            title="Student accommodation",
            location_text="Belfast"
        )
        result = check_exclusion(raw)
        # Should match student_housing first
        assert result.rule == "student_housing"
