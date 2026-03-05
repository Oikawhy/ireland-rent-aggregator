"""
AGPARS Exact Dedup Unit Tests

Tests for T073: exact duplicate detection.
"""

import pytest


class TestExactDedup:
    """Tests for exact duplicate detection."""

    def test_same_url_detected(self):
        """Two listings with same URL are duplicates."""
        from services.dedup.exact import is_exact_duplicate

        listing_a = {"url": "https://daft.ie/123", "source": "daft", "price": 1500}
        listing_b = {"url": "https://daft.ie/123", "source": "daft", "price": 1500}

        assert is_exact_duplicate(listing_a, listing_b) is True

    def test_different_url_not_duplicate(self):
        """Different URLs are not duplicates."""
        from services.dedup.exact import is_exact_duplicate

        listing_a = {"url": "https://daft.ie/123", "source": "daft", "price": 1500}
        listing_b = {"url": "https://daft.ie/456", "source": "daft", "price": 1500}

        assert is_exact_duplicate(listing_a, listing_b) is False

    def test_fingerprint_generation(self):
        """Fingerprint is deterministic for same data."""
        from services.dedup.exact import generate_fingerprint

        listing = {"url": "https://daft.ie/123", "price": 1500, "beds": 2}

        fp1 = generate_fingerprint(listing)
        fp2 = generate_fingerprint(listing)
        assert fp1 == fp2
        assert len(fp1) == 64  # SHA-256 hex

    def test_fingerprint_changes_with_data(self):
        """Different data produces different fingerprint."""
        from services.dedup.exact import generate_fingerprint

        fp1 = generate_fingerprint({"url": "https://daft.ie/123", "price": 1500})
        fp2 = generate_fingerprint({"url": "https://daft.ie/456", "price": 1500})
        assert fp1 != fp2

    def test_batch_dedup(self):
        """Batch dedup removes exact duplicates."""
        from services.dedup.exact import deduplicate_batch

        listings = [
            {"url": "https://daft.ie/1", "source": "daft", "price": 1000},
            {"url": "https://daft.ie/2", "source": "daft", "price": 2000},
            {"url": "https://daft.ie/1", "source": "daft", "price": 1000},  # duplicate
        ]

        unique = deduplicate_batch(listings)
        assert len(unique) == 2
