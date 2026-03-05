"""
Tests for services.normalizer.city_synonyms module.

Tests city resolution, Dublin postal codes, and synonym matching.
"""

from unittest.mock import patch

from services.normalizer.city_synonyms import (
    CITY_SYNONYMS,
    DUBLIN_POSTAL_CODES,
    _resolve_dublin_postal,
    _resolve_synonym,
    batch_resolve_cities,
    cached_resolve_city,
    get_resolution_stats,
)


class TestDublinPostalCodes:
    """Tests for Dublin postal code constants."""

    def test_postal_codes_defined(self):
        """Test that Dublin postal codes are defined."""
        assert isinstance(DUBLIN_POSTAL_CODES, dict)
        assert len(DUBLIN_POSTAL_CODES) > 0

    def test_d1_exists(self):
        """Test that D1 postal code exists."""
        assert "d1" in DUBLIN_POSTAL_CODES
        assert DUBLIN_POSTAL_CODES["d1"] == "Dublin 1"

    def test_all_main_districts(self):
        """Test that main Dublin districts are covered."""
        for i in range(1, 19):
            if i == 19:  # D19 doesn't exist traditionally
                continue
            key = f"d{i}"
            assert key in DUBLIN_POSTAL_CODES, f"Missing {key}"

    def test_d6w_exists(self):
        """Test that D6W (Dublin 6 West) exists."""
        assert "d6w" in DUBLIN_POSTAL_CODES


class TestCitySynonyms:
    """Tests for city synonyms constants."""

    def test_synonyms_defined(self):
        """Test that city synonyms are defined."""
        assert isinstance(CITY_SYNONYMS, dict)
        assert len(CITY_SYNONYMS) > 0

    def test_dublin_variations(self):
        """Test Dublin city variations exist."""
        assert "dublin city" in CITY_SYNONYMS
        assert CITY_SYNONYMS["dublin city"] == "Dublin"

    def test_cork_variations(self):
        """Test Cork city variations exist."""
        assert "cork city" in CITY_SYNONYMS

    def test_galway_variations(self):
        """Test Galway city variations exist."""
        assert "galway city" in CITY_SYNONYMS


class TestResolveDublinPostal:
    """Tests for _resolve_dublin_postal function."""

    def test_none_for_non_dublin(self):
        """Test that non-Dublin locations return None."""
        result = _resolve_dublin_postal("cork city centre")
        assert result is None

    def test_d1_pattern(self):
        """Test D1 pattern matching."""
        result = _resolve_dublin_postal("d1")
        assert result == "Dublin 1"

    def test_d15_pattern(self):
        """Test D15 pattern matching."""
        result = _resolve_dublin_postal("d15")
        assert result == "Dublin 15"

    def test_dublin_space_number(self):
        """Test 'Dublin 4' pattern."""
        result = _resolve_dublin_postal("dublin 4")
        assert result == "Dublin 4"

    def test_dublin_no_space(self):
        """Test 'Dublin4' pattern (no space)."""
        result = _resolve_dublin_postal("dublin4")
        assert result == "Dublin 4"

    def test_in_longer_text(self):
        """Test Dublin postal code in longer text."""
        result = _resolve_dublin_postal("apartment in d2, city centre")
        assert result == "Dublin 2"


class TestResolveSynonym:
    """Tests for _resolve_synonym function."""

    def test_none_for_unknown(self):
        """Test that unknown text returns None."""
        result = _resolve_synonym("unknown random text")
        assert result is None

    def test_exact_match(self):
        """Test exact synonym match."""
        result = _resolve_synonym("dublin city")
        assert result == "Dublin"

    def test_partial_match(self):
        """Test partial synonym match."""
        result = _resolve_synonym("near galway city centre")
        assert result == "Galway"

    def test_cork_city(self):
        """Test Cork city synonym."""
        result = _resolve_synonym("cork city")
        assert result == "Cork"

    def test_case_insensitive(self):
        """Test that matching is case insensitive."""
        result = _resolve_synonym("DUBLIN CITY")
        # Function lowercases input so should match
        assert result == "Dublin"


class TestBatchResolveCities:
    """Tests for batch_resolve_cities function."""

    @patch("services.normalizer.city_synonyms.resolve_city")
    def test_resolves_all_locations(self, mock_resolve):
        """Test that all locations are resolved."""
        mock_resolve.return_value = None

        locations = ["Dublin", "Cork", "Galway"]
        results = batch_resolve_cities(locations)

        assert len(results) == 3
        assert mock_resolve.call_count == 3

    @patch("services.normalizer.city_synonyms.resolve_city")
    def test_empty_list(self, mock_resolve):
        """Test with empty list."""
        results = batch_resolve_cities([])

        assert results == []
        assert mock_resolve.call_count == 0


class TestCachedResolveCity:
    """Tests for cached_resolve_city function."""

    def test_function_exists(self):
        """Test that cached function exists."""
        assert callable(cached_resolve_city)

    def test_has_cache(self):
        """Test that function has cache."""
        assert hasattr(cached_resolve_city, "cache_info")


class TestGetResolutionStats:
    """Tests for get_resolution_stats function."""

    def test_returns_dict(self):
        """Test that function returns a dict."""
        stats = get_resolution_stats()
        assert isinstance(stats, dict)

    def test_has_expected_keys(self):
        """Test that stats has expected keys."""
        stats = get_resolution_stats()

        assert "hits" in stats
        assert "misses" in stats
        assert "size" in stats
        assert "max_size" in stats

    def test_values_are_integers(self):
        """Test that stats values are integers."""
        stats = get_resolution_stats()

        assert isinstance(stats["hits"], int)
        assert isinstance(stats["misses"], int)
        assert isinstance(stats["size"], int)
        assert isinstance(stats["max_size"], int)
