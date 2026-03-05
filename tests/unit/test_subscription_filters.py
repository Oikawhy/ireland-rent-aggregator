"""
Tests for Subscription Filters

T032 - Tests for services/rules/subscription_filters.py
"""

from decimal import Decimal

from services.rules.subscription_filters import (
    filter_listings_for_subscription,
    matches_subscription,
)

# ═══════════════════════════════════════════════════════════════════════════════
# BUDGET FILTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestBudgetFilter:
    """Tests for budget-based filtering."""

    def test_within_budget(self):
        """Listing within budget matches."""
        listing = {"price": Decimal("1500")}
        subscription = {"filters": {"min_budget": 1000, "max_budget": 2000}}

        assert matches_subscription(listing, subscription)

    def test_below_min_budget(self):
        """Listing below min budget doesn't match."""
        listing = {"price": Decimal("800")}
        subscription = {"filters": {"min_budget": 1000, "max_budget": 2000}}

        assert not matches_subscription(listing, subscription)

    def test_above_max_budget(self):
        """Listing above max budget doesn't match."""
        listing = {"price": Decimal("2500")}
        subscription = {"filters": {"min_budget": 1000, "max_budget": 2000}}

        assert not matches_subscription(listing, subscription)

    def test_at_min_budget(self):
        """Listing at exactly min budget matches."""
        listing = {"price": 1000}
        subscription = {"filters": {"min_budget": 1000, "max_budget": 2000}}

        assert matches_subscription(listing, subscription)

    def test_at_max_budget(self):
        """Listing at exactly max budget matches."""
        listing = {"price": 2000}
        subscription = {"filters": {"min_budget": 1000, "max_budget": 2000}}

        assert matches_subscription(listing, subscription)

    def test_no_price_with_budget_filter(self):
        """Listing without price doesn't match budget filter."""
        listing = {"price": None, "beds": 2}
        subscription = {"filters": {"min_budget": 1000}}

        assert not matches_subscription(listing, subscription)

    def test_no_budget_filter(self):
        """No budget filter matches any price."""
        listing = {"price": 5000}
        subscription = {"filters": {}}

        assert matches_subscription(listing, subscription)


# ═══════════════════════════════════════════════════════════════════════════════
# BEDROOM FILTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestBedroomFilter:
    """Tests for bedroom-based filtering."""

    def test_within_beds_range(self):
        """Listing within beds range matches."""
        listing = {"beds": 2}
        subscription = {"filters": {"min_beds": 1, "max_beds": 3}}

        assert matches_subscription(listing, subscription)

    def test_below_min_beds(self):
        """Listing below min beds doesn't match."""
        listing = {"beds": 1}
        subscription = {"filters": {"min_beds": 2}}

        assert not matches_subscription(listing, subscription)

    def test_above_max_beds(self):
        """Listing above max beds doesn't match."""
        listing = {"beds": 5}
        subscription = {"filters": {"max_beds": 3}}

        assert not matches_subscription(listing, subscription)

    def test_studio_with_zero_beds(self):
        """Studio (0 beds) matches min_beds=0."""
        listing = {"beds": 0}
        subscription = {"filters": {"min_beds": 0}}

        assert matches_subscription(listing, subscription)

    def test_no_beds_info_permissive(self):
        """No beds info matches if no strict min."""
        listing = {"beds": None, "price": 1500}
        subscription = {"filters": {"min_beds": 0}}

        assert matches_subscription(listing, subscription)


# ═══════════════════════════════════════════════════════════════════════════════
# PROPERTY TYPE FILTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestPropertyTypeFilter:
    """Tests for property type filtering."""

    def test_allowed_property_type(self):
        """Matching property type passes filter."""
        listing = {"property_type": "apartment"}
        subscription = {"filters": {"property_types": ["apartment", "house"]}}

        assert matches_subscription(listing, subscription)

    def test_disallowed_property_type(self):
        """Non-matching property type fails filter."""
        listing = {"property_type": "studio"}
        subscription = {"filters": {"property_types": ["apartment", "house"]}}

        assert not matches_subscription(listing, subscription)

    def test_case_insensitive_property_type(self):
        """Property type matching is case-insensitive."""
        listing = {"property_type": "APARTMENT"}
        subscription = {"filters": {"property_types": ["apartment"]}}

        assert matches_subscription(listing, subscription)

    def test_unknown_type_matches(self):
        """Unknown property type matches (permissive default)."""
        listing = {"property_type": None}
        subscription = {"filters": {"property_types": ["apartment"]}}

        assert matches_subscription(listing, subscription)


# ═══════════════════════════════════════════════════════════════════════════════
# FURNISHED FILTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestFurnishedFilter:
    """Tests for furnished filtering."""

    def test_furnished_matches(self):
        """Furnished listing matches furnished filter."""
        listing = {"furnished": True}
        subscription = {"filters": {"furnished": True}}

        assert matches_subscription(listing, subscription)

    def test_furnished_mismatch(self):
        """Unfurnished listing doesn't match furnished filter."""
        listing = {"furnished": False}
        subscription = {"filters": {"furnished": True}}

        assert not matches_subscription(listing, subscription)

    def test_unknown_furnished_matches(self):
        """Unknown furnished status matches (permissive)."""
        listing = {"furnished": None}
        subscription = {"filters": {"furnished": True}}

        assert matches_subscription(listing, subscription)


# ═══════════════════════════════════════════════════════════════════════════════
# LOCATION FILTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestLocationFilter:
    """Tests for location-based filtering."""

    def test_city_id_match(self):
        """Listing in target city matches."""
        listing = {"city_id": 123}
        subscription = {"filters": {"city_ids": [123, 456]}}

        assert matches_subscription(listing, subscription)

    def test_city_id_mismatch(self):
        """Listing not in target cities doesn't match."""
        listing = {"city_id": 789}
        subscription = {"filters": {"city_ids": [123, 456]}}

        assert not matches_subscription(listing, subscription)

    def test_county_match(self):
        """Listing in target county matches."""
        listing = {"county": "Dublin"}
        subscription = {"filters": {"counties": ["Dublin", "Cork"]}}

        assert matches_subscription(listing, subscription)

    def test_county_case_insensitive(self):
        """County matching is case-insensitive."""
        listing = {"county": "DUBLIN"}
        subscription = {"filters": {"counties": ["dublin"]}}

        assert matches_subscription(listing, subscription)


# ═══════════════════════════════════════════════════════════════════════════════
# COMBINED FILTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestCombinedFilters:
    """Tests for multiple filters combined."""

    def test_all_filters_match(self):
        """Listing matching all filters passes."""
        listing = {
            "price": 1500,
            "beds": 2,
            "property_type": "apartment",
            "city_id": 123,
        }
        subscription = {
            "filters": {
                "min_budget": 1000,
                "max_budget": 2000,
                "min_beds": 1,
                "max_beds": 3,
                "property_types": ["apartment"],
                "city_ids": [123],
            }
        }

        assert matches_subscription(listing, subscription)

    def test_one_filter_fails(self):
        """If any filter fails, subscription doesn't match."""
        listing = {
            "price": 1500,
            "beds": 5,  # Fails max_beds
            "property_type": "apartment",
            "city_id": 123,
        }
        subscription = {
            "filters": {
                "max_budget": 2000,
                "max_beds": 3,
            }
        }

        assert not matches_subscription(listing, subscription)

    def test_no_filters_matches_all(self):
        """Empty filters match everything."""
        listing = {"price": 9999, "beds": 99}
        subscription = {"filters": {}}

        assert matches_subscription(listing, subscription)

    def test_missing_filters_key(self):
        """Missing filters key matches everything."""
        listing = {"price": 1500}
        subscription = {}

        assert matches_subscription(listing, subscription)


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH FILTER TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestBatchFilter:
    """Tests for batch filtering."""

    def test_filter_listings(self):
        """Filter a batch of listings for a subscription."""
        listings = [
            {"id": 1, "price": 1500, "beds": 2},
            {"id": 2, "price": 3000, "beds": 2},  # Over budget
            {"id": 3, "price": 1200, "beds": 1},  # Under min beds
            {"id": 4, "price": 1800, "beds": 2},
        ]
        subscription = {
            "filters": {
                "max_budget": 2000,
                "min_beds": 2,
            }
        }

        matched = filter_listings_for_subscription(listings, subscription)
        assert len(matched) == 2
        assert matched[0]["id"] == 1
        assert matched[1]["id"] == 4
