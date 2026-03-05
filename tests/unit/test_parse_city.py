"""
Unit tests for parse_city_from_area() in pub_sync.

Tests Irish address patterns across all 6 source sites.
"""

import pytest
from services.publisher.pub_sync import parse_city_from_area


class TestParseCityFromArea:
    """Test city extraction from Irish address strings."""

    # ── Standard: "Street, Area, Co. County" ──

    def test_full_address_with_co(self):
        result = parse_city_from_area(
            "5 Maypark Mews, Dunmore Road, Ardkeen, Co. Waterford", "Waterford"
        )
        assert result == "Ardkeen"

    def test_town_with_co(self):
        result = parse_city_from_area("Virginia, Co. Cavan", "Cavan")
        assert result == "Virginia"

    def test_area_within_town(self):
        result = parse_city_from_area(
            "Rahan, Edenderry, Co. Kildare", "Kildare"
        )
        assert result == "Edenderry"

    # ── Dublin postal codes ──

    def test_dublin_postal_code(self):
        result = parse_city_from_area("Dublin 8", "Dublin")
        assert result == "Dublin 8"

    def test_rathmines_dublin_6(self):
        result = parse_city_from_area("Apt 3, Rathmines, Dublin 6", "Dublin")
        assert result == "Rathmines"

    def test_dublin_area_with_postal(self):
        result = parse_city_from_area(
            "12 Main Street, Drumcondra, Dublin 9", "Dublin"
        )
        assert result == "Drumcondra"

    # ── Single-word areas ──

    def test_single_city_name(self):
        result = parse_city_from_area("Galway", "Galway")
        # Only the county name → fallback to area_text
        assert result == "Galway"

    def test_cork_city_centre(self):
        result = parse_city_from_area("Cork City Centre", "Cork")
        assert result == "Cork City Centre"

    # ── County variants ──

    def test_county_prefix(self):
        result = parse_city_from_area("Tralee, County Kerry", "Kerry")
        assert result == "Tralee"

    def test_co_no_dot(self):
        result = parse_city_from_area("Killarney, Co Kerry", "Kerry")
        assert result == "Killarney"

    def test_lowercase_co(self):
        result = parse_city_from_area("Virginia,co. Cavan", "Cavan")
        assert result == "Virginia"

    # ── Edge cases ──

    def test_none_area_text(self):
        assert parse_city_from_area(None, "Dublin") is None

    def test_empty_area_text(self):
        assert parse_city_from_area("", "Dublin") is None

    def test_no_county(self):
        result = parse_city_from_area("Sandymount, Dublin 4", None)
        assert result == "Sandymount"

    def test_multiple_areas(self):
        result = parse_city_from_area(
            "Unit 5, Ballybrack, Shankill, Co. Dublin", "Dublin"
        )
        assert result == "Shankill"
