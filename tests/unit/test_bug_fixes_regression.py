"""
FIX-09 Regression Tests for AGPARS Bug Fixes

Tests specifically targeting each fix to prevent regressions.
"""

import inspect
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

# =============================================================================
# FIX-02: Metrics Labels Tests
# =============================================================================


class TestMetricsLabelsRegression:
    """Tests for FIX-02: proper metrics labels (reason + source)."""

    def test_exclusion_metrics_has_reason_and_source_labels(self):
        """Verify metrics increment uses reason and source labels."""
        from services.rules.apply_rules import apply_rules

        mock_raw_listing = MagicMock()
        mock_raw_listing.source = "daft"
        mock_raw_listing.title = "Student Accommodation Dublin"
        mock_raw_listing.description = ""

        mock_normalized = MagicMock()
        mock_normalized.lease_length_months = None

        with patch("services.rules.apply_rules.check_exclusion") as mock_check:
            mock_exclusion = MagicMock()
            mock_exclusion.is_excluded = True
            mock_exclusion.rule = "student_housing"
            mock_exclusion.reason = "Student housing detected"
            mock_check.return_value = mock_exclusion

            with patch("services.rules.apply_rules.get_metrics") as mock_get_metrics:
                mock_metrics = MagicMock()
                mock_get_metrics.return_value = mock_metrics

                apply_rules(mock_raw_listing, mock_normalized)

                # Verify correct labels
                mock_metrics.increment.assert_called_once()
                call_args = mock_metrics.increment.call_args
                assert call_args[1]["labels"]["reason"] == "student_housing"
                assert call_args[1]["labels"]["source"] == "daft"

    def test_exclusion_metrics_logs_errors_instead_of_silent_pass(self):
        """Verify metrics errors are logged, not silently caught."""
        from services.rules.apply_rules import apply_rules

        mock_raw_listing = MagicMock()
        mock_raw_listing.source = "rent"

        mock_normalized = MagicMock()

        with patch("services.rules.apply_rules.check_exclusion") as mock_check:
            mock_exclusion = MagicMock()
            mock_exclusion.is_excluded = True
            mock_exclusion.rule = "test_rule"
            mock_exclusion.reason = "Test reason"
            mock_check.return_value = mock_exclusion

            with patch("services.rules.apply_rules.get_metrics") as mock_get_metrics:
                mock_get_metrics.side_effect = Exception("Metrics error")

                with patch("services.rules.apply_rules.logger") as mock_logger:
                    # Should not raise, but should log warning
                    result = apply_rules(mock_raw_listing, mock_normalized)

                    assert result == "Test reason"
                    mock_logger.warning.assert_called()
                    assert "metrics" in str(mock_logger.warning.call_args).lower()


# =============================================================================
# FIX-03/FIX-10: State Machine Tests
# =============================================================================


class TestStateMachineRegression:
    """Tests for FIX-03/FIX-10: strict outbox state machine."""

    @patch("services.publisher.event_outbox.get_session_context")
    def test_delivered_requires_delivering_status(self, mock_get_session):
        """mark_event_delivered must require status=delivering."""
        from services.publisher.event_outbox import mark_event_delivered

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 0  # No rows updated

        result = mark_event_delivered(123)

        assert result is False  # Should fail for invalid transition

        # Verify SQL contains status check
        call_args = mock_session.execute.call_args
        sql_text = str(call_args[0][0])
        assert "delivering" in sql_text.lower()

    @patch("services.publisher.event_outbox.get_session_context")
    def test_retry_only_failed_events(self, mock_get_session):
        """retry_failed_events must only transition FAILED events."""
        from services.publisher.event_outbox import retry_failed_events

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 2

        result = retry_failed_events(max_retries=5)

        assert result == 2

        # Verify SQL has explicit status check with cast
        call_args = mock_session.execute.call_args
        sql_text = str(call_args[0][0])
        assert "'failed'" in sql_text.lower()
        assert "eventstatus" in sql_text.lower() or "status" in sql_text.lower()

    @patch("services.publisher.event_outbox.get_session_context")
    def test_invalid_transition_logged(self, mock_get_session):
        """Invalid state transitions should be logged with warning."""
        from services.publisher.event_outbox import mark_event_delivered

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 0

        with patch("services.publisher.event_outbox.logger") as mock_logger:
            result = mark_event_delivered(999)

            assert result is False
            mock_logger.warning.assert_called()


# =============================================================================
# FIX-04: Insert/Update Stats Tests
# =============================================================================


class TestPubSyncStatsRegression:
    """Tests for FIX-04: tracking inserted vs updated counts."""

    @patch("services.publisher.pub_sync.get_session_context")
    def test_sync_returns_inserted_and_updated_counts(self, mock_get_session):
        """sync_listings_to_pub must track inserted and updated separately."""
        from services.publisher.pub_sync import sync_listings_to_pub

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate no rows returned
        mock_session.execute.return_value.fetchall.return_value = []

        stats = sync_listings_to_pub()

        # Stats should have all required keys
        assert "inserted" in stats
        assert "updated" in stats
        assert "processed" in stats
        assert "errors" in stats

    def test_upsert_returns_insert_or_update(self):
        """_upsert_to_pub must return 'insert' or 'update'."""
        from services.publisher.pub_sync import _upsert_to_pub

        mock_session = MagicMock()

        # Simulate insert (xmax = 0)
        mock_session.execute.return_value.scalar.return_value = True

        mock_row = MagicMock()
        mock_row.raw_id = 1
        mock_row.source = "daft"
        mock_row.url = "https://example.com"
        mock_row.first_photo_url = None
        mock_row.price = 1500
        mock_row.beds = 2
        mock_row.baths = 1
        mock_row.property_type = "apartment"
        mock_row.county = "Dublin"
        mock_row.city_name = "Dublin"
        mock_row.area_text = "Dublin 2"
        mock_row.first_seen = None
        mock_row.status = "active"

        result = _upsert_to_pub(mock_session, mock_row)

        assert result == "insert"

        # Simulate update (xmax != 0)
        mock_session.execute.return_value.scalar.return_value = False
        result = _upsert_to_pub(mock_session, mock_row)
        assert result == "update"


# =============================================================================
# FIX-07: Batching Retention Tests
# =============================================================================


class TestRetentionBatchingRegression:
    """Tests for FIX-07: batching in retention cleanup."""

    def test_cleanup_delivered_events_has_batch_size_param(self):
        """cleanup_delivered_events must accept batch_size parameter."""
        from services.maintenance.retention import cleanup_delivered_events

        sig = inspect.signature(cleanup_delivered_events)
        assert "batch_size" in sig.parameters

    def test_cleanup_dead_events_has_batch_size_param(self):
        """cleanup_dead_events must accept batch_size parameter."""
        from services.maintenance.retention import cleanup_dead_events

        sig = inspect.signature(cleanup_dead_events)
        assert "batch_size" in sig.parameters

    def test_cleanup_delivery_logs_has_batch_size_param(self):
        """cleanup_delivery_logs must accept batch_size parameter."""
        from services.maintenance.retention import cleanup_delivery_logs

        sig = inspect.signature(cleanup_delivery_logs)
        assert "batch_size" in sig.parameters

    def test_cleanup_job_logs_has_batch_size_param(self):
        """cleanup_job_logs must accept batch_size parameter."""
        from services.maintenance.retention import cleanup_job_logs

        sig = inspect.signature(cleanup_job_logs)
        assert "batch_size" in sig.parameters


# =============================================================================
# FIX-12: DateTime Unification Tests
# =============================================================================


class TestDatetimeUnificationRegression:
    """Tests for FIX-12: using datetime.now(UTC) instead of utcnow()."""

    def test_retention_uses_utc_aware_datetime(self):
        """Retention module should use timezone-aware datetime."""
        retention_file = Path("services/maintenance/retention.py")
        if not retention_file.exists():
            retention_file = Path(__file__).parent.parent.parent / "services/maintenance/retention.py"

        if retention_file.exists():
            content = retention_file.read_text()
            # Should NOT use deprecated utcnow()
            assert "datetime.utcnow()" not in content
            # Should use datetime.now(UTC)
            assert "datetime.now(UTC)" in content
            # Should import UTC
            assert "from datetime import" in content and "UTC" in content

    def test_event_outbox_uses_utc_aware_datetime(self):
        """Event outbox module should use timezone-aware datetime."""
        outbox_file = Path("services/publisher/event_outbox.py")
        if not outbox_file.exists():
            outbox_file = Path(__file__).parent.parent.parent / "services/publisher/event_outbox.py"

        if outbox_file.exists():
            content = outbox_file.read_text()
            # Should use datetime.now(UTC)
            assert "datetime.now(UTC)" in content


# =============================================================================
# General Regression Prevention
# =============================================================================


class TestNoDeprecatedOutbox:
    """Ensure deprecated outbox module is removed."""

    def test_packages_storage_outbox_not_imported(self):
        """packages.storage.outbox should not be imported anywhere in production code."""
        # Search only in production directories, not tests
        result = subprocess.run(
            [
                "grep", "-r", "from packages.storage.outbox",
                "packages/", "services/",
                "--include=*.py",
            ],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        )

        # Should find no imports (exit code 1 means no matches)
        assert result.returncode == 1 or result.stdout.strip() == ""
