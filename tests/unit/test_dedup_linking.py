"""
AGPARS Cross-Source Linking Unit Tests

Tests for T074: cross-source duplicate detection.
"""

import pytest


class TestCrossSourceLinking:
    """Tests for cross-source duplicate linking."""

    def test_same_address_different_sources(self):
        """Listings with same address from different sources are linked."""
        from services.dedup.linker import compute_similarity

        listing_a = {
            "url": "https://daft.ie/123",
            "source": "daft",
            "price": 1500,
            "beds": 2,
            "county": "Dublin",
            "city": "Dublin",
            "area_text": "123 Main Street, Dublin 2",
        }
        listing_b = {
            "url": "https://rent.ie/456",
            "source": "rent",
            "price": 1500,
            "beds": 2,
            "county": "Dublin",
            "city": "Dublin",
            "area_text": "123 Main St, Dublin 2",
        }

        score = compute_similarity(listing_a, listing_b)
        assert score >= 0.7  # High similarity

    def test_different_listings_low_score(self):
        """Completely different listings have low similarity."""
        from services.dedup.linker import compute_similarity

        listing_a = {
            "source": "daft", "price": 1500, "beds": 2,
            "county": "Dublin", "city": "Dublin", "area_text": "123 Main Street",
        }
        listing_b = {
            "source": "rent", "price": 3000, "beds": 4,
            "county": "Cork", "city": "Cork", "area_text": "456 Oak Avenue",
        }

        score = compute_similarity(listing_a, listing_b)
        assert score < 0.3

    def test_cross_source_only(self):
        """Same-source listings are not cross-linked."""
        from services.dedup.linker import should_link

        listing_a = {"source": "daft", "price": 1500}
        listing_b = {"source": "daft", "price": 1500}

        assert should_link(listing_a, listing_b) is False

    def test_normalize_address(self):
        """Address normalization strips variations."""
        from services.dedup.linker import normalize_address

        assert normalize_address("123 Main Street") == normalize_address("123 Main St")
        assert normalize_address("Apt 4, 12 O'Connell St") == normalize_address("Apt. 4, 12 OConnell Street")
