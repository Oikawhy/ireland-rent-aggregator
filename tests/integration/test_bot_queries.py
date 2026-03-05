"""
AGPARS Bot Queries Integration Tests

Tests for T034.2: bot query layer (listings, filters).
"""

from unittest.mock import MagicMock, patch

import pytest


class TestListingsQuery:
    """Tests for services.bot.queries.listings."""

    @patch("services.bot.queries.listings.get_readonly_session")
    def test_get_latest_listings_no_filters(self, mock_session):
        """Returns listings without filters."""
        from services.bot.queries.listings import get_latest_listings

        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = MagicMock(return_value=session_ctx)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row._mapping = {
            "listing_id": 1,
            "raw_id": 100,
            "source": "daft",
            "url": "https://daft.ie/123",
            "price": 1500,
            "beds": 2,
            "baths": 1,
            "property_type": "apartment",
            "county": "Dublin",
            "city": "Dublin",
            "area_text": None,
            "first_photo_url": None,
            "published_at": None,
            "updated_at": None,
        }
        session_ctx.execute.return_value.fetchall.return_value = [mock_row]

        results = get_latest_listings(limit=5)
        assert len(results) == 1
        assert results[0]["source"] == "daft"
        assert results[0]["price"] == 1500

    @patch("services.bot.queries.listings.get_readonly_session")
    def test_get_latest_with_budget_filter(self, mock_session):
        """Budget filter applies correctly."""
        from services.bot.queries.listings import get_latest_listings

        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = MagicMock(return_value=session_ctx)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        session_ctx.execute.return_value.fetchall.return_value = []

        get_latest_listings(filters={"max_budget": 2000}, limit=10)

        # Verify the query includes budget condition
        call_args = session_ctx.execute.call_args
        query_text = str(call_args[0][0].text)
        assert "max_budget" in query_text or "price" in query_text

    @patch("services.bot.queries.listings.get_readonly_session")
    def test_get_listing_count(self, mock_session):
        """Count query returns scalar."""
        from services.bot.queries.listings import get_listing_count

        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = MagicMock(return_value=session_ctx)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        session_ctx.execute.return_value.scalar.return_value = 42

        count = get_listing_count()
        assert count == 42


class TestQueryFilters:
    """Tests for services.bot.queries.filters."""

    def test_build_filter_dict_all_params(self):
        """All filter parameters included in output."""
        from services.bot.queries.filters import build_filter_dict

        filters = build_filter_dict(
            min_budget=500,
            max_budget=2000,
            min_beds=2,
            counties=["dublin", "cork"],
        )

        assert filters["min_budget"] == 500
        assert filters["max_budget"] == 2000
        assert filters["min_beds"] == 2
        assert filters["counties"] == ["Dublin", "Cork"]  # title-cased

    def test_build_filter_dict_empty(self):
        """No params returns empty dict."""
        from services.bot.queries.filters import build_filter_dict

        filters = build_filter_dict()
        assert filters == {}

    def test_merge_filters(self):
        """New filters override existing."""
        from services.bot.queries.filters import merge_filters

        existing = {"max_budget": 1500, "min_beds": 1}
        new = {"max_budget": 2000, "counties": ["Dublin"]}

        merged = merge_filters(existing, new)
        assert merged["max_budget"] == 2000
        assert merged["min_beds"] == 1
        assert merged["counties"] == ["Dublin"]

    def test_merge_filters_clear(self):
        """Empty list clears filter."""
        from services.bot.queries.filters import merge_filters

        existing = {"counties": ["Dublin", "Cork"]}
        merged = merge_filters(existing, {"counties": []})
        assert "counties" not in merged

    def test_describe_filters(self):
        """Human-readable description generated."""
        from services.bot.queries.filters import describe_filters

        desc = describe_filters({
            "max_budget": 2000,
            "min_beds": 2,
            "counties": ["Dublin"],
        })

        assert "2,000" in desc
        assert "2+" in desc
        assert "Dublin" in desc

    def test_describe_empty(self):
        """Empty filters returns fallback text."""
        from services.bot.queries.filters import describe_filters

        desc = describe_filters({})
        assert "No filters" in desc
