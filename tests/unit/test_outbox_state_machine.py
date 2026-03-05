"""
Tests for Event Outbox State Machine

P3-07 - Tests for all state transitions in the outbox
"""

from unittest.mock import MagicMock, patch

import pytest


class TestEventOutboxStateMachine:
    """Tests for outbox state machine transitions."""

    @patch("services.publisher.event_outbox.get_session_context")
    def test_create_event_sets_pending_status(self, mock_get_session):
        """New events start with PENDING status."""
        from services.publisher.event_outbox import EventType, create_event

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.scalar_one.return_value = 123

        # Should not raise
        event_id = create_event(
            workspace_id=1,
            event_type=EventType.NEW,
            listing_raw_id=100,
            payload={"test": "data"},
        )

        assert event_id == 123

        # Verify SQL contains 'pending' status
        call_args = mock_session.execute.call_args
        sql_text = str(call_args[0][0])
        assert "'pending'" in sql_text.lower()

    @patch("services.publisher.event_outbox.get_session_context")
    def test_mark_delivering_transition(self, mock_get_session):
        """Test PENDING → DELIVERING transition (strict state machine)."""
        from services.publisher.event_outbox import mark_event_delivering

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 1

        result = mark_event_delivering(123)

        assert result is True
        call_args = mock_session.execute.call_args
        sql_text = str(call_args[0][0])
        assert "delivering" in sql_text.lower()
        # Per ARCHITECT.md: only PENDING → DELIVERING transition allowed
        assert "pending" in sql_text.lower()
        assert "failed" not in sql_text.lower()  # Strict: no FAILED → DELIVERING

    @patch("services.publisher.event_outbox.get_session_context")
    def test_mark_delivered_transition(self, mock_get_session):
        """Test DELIVERING → DELIVERED transition."""
        from services.publisher.event_outbox import mark_event_delivered

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 1

        result = mark_event_delivered(123)

        assert result is True
        call_args = mock_session.execute.call_args
        sql_text = str(call_args[0][0])
        assert "delivered" in sql_text.lower()

    @patch("services.publisher.event_outbox.get_session_context")
    def test_mark_failed_increments_retry_count(self, mock_get_session):
        """Test DELIVERING → FAILED transition with retry increment."""
        from services.publisher.event_outbox import mark_event_failed

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        # Current retry count = 2
        mock_row = MagicMock()
        mock_row.retry_count = 2
        mock_session.execute.return_value.fetchone.return_value = mock_row

        result = mark_event_failed(123, max_retries=5)

        assert result is True
        # Should have called execute twice: once for SELECT, once for UPDATE
        assert mock_session.execute.call_count == 2

    @patch("services.publisher.event_outbox.get_session_context")
    def test_mark_failed_becomes_dead_after_max_retries(self, mock_get_session):
        """Test FAILED → DEAD when retry_count >= max_retries."""
        from services.publisher.event_outbox import mark_event_failed

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

        # Current retry count = 4, max_retries = 5 → becomes DEAD
        mock_row = MagicMock()
        mock_row.retry_count = 4
        mock_session.execute.return_value.fetchone.return_value = mock_row

        result = mark_event_failed(123, max_retries=5)

        assert result is True
        # Verify "dead" is in the final UPDATE
        update_call = mock_session.execute.call_args_list[1]
        params = update_call[0][1]
        assert params["status"] == "dead"
        assert params["retry_count"] == 5

    @patch("services.publisher.event_outbox.get_session_context")
    def test_retry_failed_events_moves_to_pending(self, mock_get_session):
        """Test FAILED → PENDING via retry_failed_events."""
        from services.publisher.event_outbox import retry_failed_events

        mock_session = MagicMock()
        mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_get_session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 3

        result = retry_failed_events(max_retries=5)

        assert result == 3
        call_args = mock_session.execute.call_args
        sql_text = str(call_args[0][0])
        assert "pending" in sql_text.lower()
        assert "failed" in sql_text.lower()


class TestEventOutboxValidation:
    """Tests for event validation."""

    def test_invalid_event_type_raises_error(self):
        """Invalid event type should raise ValueError."""
        from services.publisher.event_outbox import create_event

        with pytest.raises(ValueError, match="Invalid event_type"):
            create_event(
                workspace_id=1,
                event_type="invalid_type",
                listing_raw_id=100,
            )

    def test_event_type_enum_values(self):
        """EventType enum has correct values."""
        from services.publisher.event_outbox import EventType

        assert EventType.NEW.value == "new"
        assert EventType.UPDATED.value == "updated"

    def test_event_status_enum_values(self):
        """EventStatus enum has correct values."""
        from services.publisher.event_outbox import EventStatus

        assert EventStatus.PENDING.value == "pending"
        assert EventStatus.DELIVERING.value == "delivering"
        assert EventStatus.DELIVERED.value == "delivered"
        assert EventStatus.FAILED.value == "failed"
        assert EventStatus.DEAD.value == "dead"
