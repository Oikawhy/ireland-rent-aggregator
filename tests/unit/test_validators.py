"""
Tests for services.normalizer.validators module.

Tests validation functions for listings data.
"""


from services.normalizer.validators import (
    MAX_BATHS,
    MAX_BEDS,
    MAX_PRICE,
    MIN_BATHS,
    MIN_BEDS,
    MIN_PRICE,
    get_validation_summary,
    is_valid_listing,
    validate_baths,
    validate_beds,
    validate_listing,
    validate_location,
    validate_price,
    validate_url,
)


class TestValidationConstants:
    """Tests for validation constants."""

    def test_price_range_reasonable(self):
        """Test that price range is reasonable."""
        assert MIN_PRICE >= 0
        assert MAX_PRICE > MIN_PRICE
        assert MIN_PRICE <= 500  # Should allow low prices
        assert MAX_PRICE >= 10000  # Should allow high prices

    def test_beds_range_reasonable(self):
        """Test that beds range is reasonable."""
        assert MIN_BEDS >= 0  # Studio
        assert MAX_BEDS > MIN_BEDS
        assert MAX_BEDS <= 50  # Reasonable upper limit

    def test_baths_range_reasonable(self):
        """Test that baths range is reasonable."""
        assert MIN_BATHS >= 0
        assert MAX_BATHS > MIN_BATHS


class TestValidatePrice:
    """Tests for validate_price function."""

    def test_none_price_is_valid(self):
        """Test that None price is valid (optional field)."""
        errors = validate_price(None)
        assert errors == []

    def test_valid_price(self):
        """Test valid price returns no errors."""
        errors = validate_price(1500)
        assert errors == []

    def test_valid_price_as_string(self):
        """Test valid price as string returns no errors."""
        errors = validate_price("1500")
        assert errors == []

    def test_price_too_low(self):
        """Test that price below MIN_PRICE generates error."""
        errors = validate_price(10)
        assert len(errors) > 0
        assert any("low" in e.lower() for e in errors)

    def test_price_too_high(self):
        """Test that price above MAX_PRICE generates error."""
        errors = validate_price(100000)
        assert len(errors) > 0
        assert any("high" in e.lower() for e in errors)

    def test_invalid_price_format(self):
        """Test that invalid price format generates error."""
        errors = validate_price("not a number")
        assert len(errors) > 0
        assert any("invalid" in e.lower() or "format" in e.lower() for e in errors)

    def test_boundary_prices(self):
        """Test prices at boundaries."""
        # At minimum
        errors = validate_price(MIN_PRICE)
        assert errors == []

        # At maximum
        errors = validate_price(MAX_PRICE)
        assert errors == []


class TestValidateBeds:
    """Tests for validate_beds function."""

    def test_none_beds_is_valid(self):
        """Test that None beds is valid (optional field)."""
        errors = validate_beds(None)
        assert errors == []

    def test_valid_beds(self):
        """Test valid bedroom count returns no errors."""
        errors = validate_beds(2)
        assert errors == []

    def test_studio_zero_beds(self):
        """Test that 0 beds (studio) is valid."""
        errors = validate_beds(0)
        assert errors == []

    def test_beds_too_high(self):
        """Test that beds above MAX_BEDS generates error."""
        errors = validate_beds(100)
        assert len(errors) > 0

    def test_negative_beds_invalid(self):
        """Test that negative beds generates error."""
        errors = validate_beds(-1)
        assert len(errors) > 0

    def test_invalid_beds_format(self):
        """Test that invalid beds format generates error."""
        errors = validate_beds("not a number")
        assert len(errors) > 0


class TestValidateBaths:
    """Tests for validate_baths function."""

    def test_none_baths_is_valid(self):
        """Test that None baths is valid (optional field)."""
        errors = validate_baths(None)
        assert errors == []

    def test_valid_baths(self):
        """Test valid bathroom count returns no errors."""
        errors = validate_baths(2)
        assert errors == []

    def test_baths_too_high(self):
        """Test that baths above MAX_BATHS generates error."""
        errors = validate_baths(50)
        assert len(errors) > 0

    def test_negative_baths_invalid(self):
        """Test that negative baths generates error."""
        errors = validate_baths(-1)
        assert len(errors) > 0


class TestValidateLocation:
    """Tests for validate_location function."""

    def test_valid_location_with_city_id(self):
        """Test valid location with city_id."""
        data = {"city_id": 1, "county": None, "area_text": None}
        errors = validate_location(data)
        assert errors == []

    def test_valid_location_with_county(self):
        """Test valid location with county."""
        data = {"city_id": None, "county": "Dublin", "area_text": None}
        errors = validate_location(data)
        assert errors == []

    def test_valid_location_with_area_text(self):
        """Test valid location with area_text."""
        data = {"city_id": None, "county": None, "area_text": "Dublin 1"}
        errors = validate_location(data)
        assert errors == []

    def test_no_location_generates_error(self):
        """Test that missing all location info generates error."""
        data = {"city_id": None, "county": None, "area_text": None}
        errors = validate_location(data)
        assert len(errors) > 0
        assert any("location" in e.lower() for e in errors)

    def test_empty_dict(self):
        """Test empty dict generates error."""
        errors = validate_location({})
        assert len(errors) > 0


class TestValidateUrl:
    """Tests for validate_url function."""

    def test_valid_https_url(self):
        """Test valid HTTPS URL returns no errors."""
        errors = validate_url("https://www.daft.ie/property/123")
        assert errors == []

    def test_valid_http_url(self):
        """Test valid HTTP URL returns no errors."""
        errors = validate_url("http://www.daft.ie/property/123")
        assert errors == []

    def test_missing_url_generates_error(self):
        """Test that missing URL generates error."""
        errors = validate_url(None)
        assert len(errors) > 0
        assert any("required" in e.lower() for e in errors)

    def test_empty_url_generates_error(self):
        """Test that empty URL generates error."""
        errors = validate_url("")
        assert len(errors) > 0

    def test_invalid_url_format(self):
        """Test that invalid URL format generates error."""
        errors = validate_url("not-a-url")
        assert len(errors) > 0
        assert any("invalid" in e.lower() or "format" in e.lower() for e in errors)

    def test_url_with_spaces_generates_error(self):
        """Test that URL with spaces generates error."""
        errors = validate_url("https://www.daft.ie/property 123")
        assert len(errors) > 0
        assert any("space" in e.lower() for e in errors)

    def test_url_too_long(self):
        """Test that very long URL generates error."""
        long_url = "https://www.daft.ie/" + "a" * 3000
        errors = validate_url(long_url)
        assert len(errors) > 0
        assert any("long" in e.lower() for e in errors)


class TestValidateListing:
    """Tests for validate_listing function."""

    def test_valid_listing_dict(self):
        """Test valid listing dict returns no errors."""
        listing = {
            "price": 1500,
            "beds": 2,
            "baths": 1,
            "city_id": 1,
            "county": "Dublin",
            "area_text": "Dublin 1",
        }
        errors = validate_listing(listing)
        assert errors == []

    def test_invalid_listing_dict(self):
        """Test invalid listing dict returns errors."""
        listing = {
            "price": -100,  # Invalid
            "beds": 100,  # Invalid
            "baths": 50,  # Invalid
            # No location
        }
        errors = validate_listing(listing)
        assert len(errors) > 0

    def test_partial_listing(self):
        """Test partial listing with only required fields."""
        listing = {"county": "Dublin"}
        errors = validate_listing(listing)
        # Should have no errors if location is present
        assert errors == []


class TestIsValidListing:
    """Tests for is_valid_listing function."""

    def test_valid_listing_returns_true(self):
        """Test valid listing returns True."""
        listing = {"county": "Dublin", "price": 1500}
        assert is_valid_listing(listing) is True

    def test_invalid_listing_returns_false(self):
        """Test invalid listing returns False."""
        listing = {"price": -100}  # Invalid price, no location
        assert is_valid_listing(listing) is False


class TestGetValidationSummary:
    """Tests for get_validation_summary function."""

    def test_all_valid_listings(self):
        """Test summary with all valid listings."""
        listings = [
            {"county": "Dublin"},
            {"county": "Cork"},
            {"city_id": 1},
        ]
        summary = get_validation_summary(listings)

        assert summary["total"] == 3
        assert summary["valid"] == 3
        assert summary["invalid"] == 0

    def test_all_invalid_listings(self):
        """Test summary with all invalid listings."""
        listings = [
            {"price": -100},  # Invalid - no location
            {"beds": 100},  # Invalid - no location
        ]
        summary = get_validation_summary(listings)

        assert summary["total"] == 2
        assert summary["invalid"] == 2
        assert summary["valid"] == 0

    def test_mixed_listings(self):
        """Test summary with mixed valid/invalid listings."""
        listings = [
            {"county": "Dublin"},  # Valid
            {},  # Invalid - no location
        ]
        summary = get_validation_summary(listings)

        assert summary["total"] == 2
        assert summary["valid"] == 1
        assert summary["invalid"] == 1

    def test_empty_list(self):
        """Test summary with empty list."""
        summary = get_validation_summary([])

        assert summary["total"] == 0
        assert summary["valid"] == 0
        assert summary["invalid"] == 0

    def test_error_breakdown_populated(self):
        """Test that error_breakdown is populated."""
        listings = [{}]  # No location
        summary = get_validation_summary(listings)

        assert "error_breakdown" in summary
        assert isinstance(summary["error_breakdown"], dict)
