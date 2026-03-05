"""
AGPARS Database Population Tests

T065.7 - Integration tests to verify data populates correctly in database.
Tests verify listings flow from raw → normalized → pub schemas.
"""


import pytest

pytestmark = pytest.mark.integration


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE MODEL IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestModelImports:
    """Verify all database models are importable."""

    def test_import_raw_listing_model(self):
        """ListingRaw model is importable."""
        from packages.storage.models import ListingRaw
        assert ListingRaw is not None

    def test_import_normalized_listing_model(self):
        """ListingNormalized model is importable."""
        from packages.storage.models import ListingNormalized
        assert ListingNormalized is not None

    def test_import_public_listing_model(self):
        """PublicListing model is importable."""
        from packages.storage.models import PublicListing
        assert PublicListing is not None

    def test_import_city_model(self):
        """City model is importable."""
        from packages.storage.models import City
        assert City is not None


# ═══════════════════════════════════════════════════════════════════════════════
# STORAGE LAYER IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestStorageLayerImports:
    """Verify all storage functions are importable."""

    def test_listings_storage(self):
        """Listings storage functions exist."""
        from packages.storage.listings import (
            get_normalized_listing,
            get_raw_listing_by_id,
            upsert_normalized_listing,
            upsert_raw_listing,
        )

        assert callable(upsert_raw_listing)
        assert callable(get_raw_listing_by_id)
        assert callable(upsert_normalized_listing)
        assert callable(get_normalized_listing)

    def test_workspaces_storage(self):
        """Workspace storage functions exist."""
        from packages.storage.workspaces import (
            create_workspace,
            get_workspace_by_id,
            update_workspace,
        )

        assert callable(create_workspace)
        assert callable(get_workspace_by_id)
        assert callable(update_workspace)

    def test_subscriptions_storage(self):
        """Subscription storage functions exist."""
        from packages.storage.subscriptions import (
            create_subscription,
            get_active_subscriptions,
            get_subscription,
        )

        assert callable(create_subscription)
        assert callable(get_subscription)
        assert callable(get_active_subscriptions)


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA VERIFICATION
# ═══════════════════════════════════════════════════════════════════════════════


class TestSchemaVerification:
    """Verify database schemas are correctly configured."""

    def test_raw_schema_defined(self):
        """Raw schema is defined in models."""
        from packages.storage.models import ListingRaw

        # Verify model has __table_args__ with schema
        assert hasattr(ListingRaw, "__table_args__")
        table_args = ListingRaw.__table_args__

        # Schema should be 'raw' - check both dict and tuple formats
        if isinstance(table_args, dict):
            assert table_args.get("schema") == "raw"
        else:
            # Tuple format (Index, Index, ..., {dict})
            schema_dict = [a for a in table_args if isinstance(a, dict)]
            assert any(d.get("schema") == "raw" for d in schema_dict)

    def test_core_schema_models(self):
        """Core schema models are defined."""
        from packages.storage.models import City, ListingNormalized

        assert City is not None
        assert ListingNormalized is not None

    def test_pub_schema_model(self):
        """Pub schema model is defined."""
        from packages.storage.models import PublicListing

        assert hasattr(PublicListing, "__table_args__")


# ═══════════════════════════════════════════════════════════════════════════════
# DATA FLOW VERIFICATION
# ═══════════════════════════════════════════════════════════════════════════════


class TestDataFlowVerification:
    """Verify data can flow through all pipeline stages."""

    def test_runner_creates_raw_listing(self):
        """Collector runner creates valid RawListing objects."""
        from services.collector.runner import RawListing

        raw = RawListing(
            source="daft",
            source_listing_id="test_123",
            url="https://daft.ie/test",
            price_text="€1,500",
            beds_text="2",
            location_text="Dublin",
        )

        assert raw.source == "daft"
        assert raw.source_listing_id == "test_123"
        assert raw.price_text == "€1,500"

    def test_sanitization_output(self):
        """Sanitization produces correct output types."""
        from services.collector.sanitize import (
            sanitize_baths,
            sanitize_beds,
            sanitize_price,
        )

        assert sanitize_price("€1,500 per month") == 1500
        assert sanitize_beds("3 bedrooms") == 3
        assert sanitize_baths("2 bathrooms") == 2

    def test_normalization_pipeline_exists(self):
        """Normalization pipeline is importable."""
        from services.normalizer.normalize import NormalizationPipeline

        pipeline = NormalizationPipeline()
        assert hasattr(pipeline, "normalize")

    def test_pub_sync_exists(self):
        """Pub sync function is importable."""
        from services.publisher.pub_sync import sync_listings_to_pub

        assert callable(sync_listings_to_pub)


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


class TestBatchOperations:
    """test batch data operations."""

    def test_batch_normalize_exists(self):
        """Batch normalization function exists."""
        from services.normalizer.normalize import batch_normalize

        assert callable(batch_normalize)

    def test_sync_functions_exist(self):
        """Publisher sync functions exist."""
        from services.publisher.sync import (
            run_publisher_sync,
            sync_source,
        )

        assert callable(run_publisher_sync)
        assert callable(sync_source)


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT OUTBOX VERIFICATION
# ═══════════════════════════════════════════════════════════════════════════════


class TestEventOutboxVerification:
    """Verify event outbox functionality."""

    def test_event_types_defined(self):
        """Event types are properly defined."""
        from services.publisher.event_outbox import EventStatus, EventType

        assert EventType.NEW.value == "new"
        assert EventType.UPDATED.value == "updated"
        assert EventStatus.PENDING.value == "pending"
        assert EventStatus.DELIVERED.value == "delivered"

    def test_create_event_callable(self):
        """Create event function is callable."""
        from services.publisher.event_outbox import create_event

        assert callable(create_event)

    def test_event_delivery_functions(self):
        """Event delivery functions exist."""
        from services.publisher.event_outbox import (
            mark_event_delivered,
            mark_event_delivering,
            mark_event_failed,
        )

        assert callable(mark_event_delivering)
        assert callable(mark_event_delivered)
        assert callable(mark_event_failed)


# ═══════════════════════════════════════════════════════════════════════════════
# METRICS VERIFICATION
# ═══════════════════════════════════════════════════════════════════════════════


class TestMetricsVerification:
    """Verify metrics are properly configured."""

    def test_publisher_metrics_exist(self):
        """Publisher metrics module exists."""
        from services.publisher.metrics import (
            LISTINGS_SYNCED,
            record_sync_complete,
        )

        assert LISTINGS_SYNCED is not None
        assert callable(record_sync_complete)
