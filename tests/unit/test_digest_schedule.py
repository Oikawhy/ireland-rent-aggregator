"""
AGPARS Digest Schedule Unit Tests

Tests for T066: digest schedule calculation and validation.
"""

from datetime import datetime, time
from unittest.mock import patch

import pytest


class TestDigestScheduleCalculation:
    """Tests for digest_scheduler schedule logic."""

    def test_parse_schedule_daily(self):
        """Daily digest parsed correctly."""
        from services.notifier.digest_scheduler import parse_digest_schedule

        schedule = parse_digest_schedule({
            "frequency": "daily",
            "time": "09:00",
            "timezone": "Europe/Dublin",
        })

        assert schedule["frequency"] == "daily"
        assert schedule["hour"] == 9
        assert schedule["minute"] == 0

    def test_parse_schedule_weekly(self):
        """Weekly digest with day of week."""
        from services.notifier.digest_scheduler import parse_digest_schedule

        schedule = parse_digest_schedule({
            "frequency": "weekly",
            "day_of_week": "monday",
            "time": "18:30",
        })

        assert schedule["frequency"] == "weekly"
        assert schedule["day_of_week"] == 0  # Monday = 0
        assert schedule["hour"] == 18
        assert schedule["minute"] == 30

    def test_invalid_frequency(self):
        """Invalid frequency raises ValueError."""
        from services.notifier.digest_scheduler import parse_digest_schedule

        with pytest.raises(ValueError, match="frequency"):
            parse_digest_schedule({"frequency": "hourly"})

    def test_is_due_daily(self):
        """Daily digest due at correct time."""
        from services.notifier.digest_scheduler import is_digest_due

        schedule = {
            "frequency": "daily",
            "hour": 9,
            "minute": 0,
            "timezone": "Europe/Dublin",
        }

        # Mock current time to 09:00
        mock_now = datetime(2025, 1, 15, 9, 0, 0)
        assert is_digest_due(schedule, mock_now) is True

    def test_is_due_daily_wrong_time(self):
        """Daily digest not due at wrong time."""
        from services.notifier.digest_scheduler import is_digest_due

        schedule = {
            "frequency": "daily",
            "hour": 9,
            "minute": 0,
            "timezone": "Europe/Dublin",
        }

        mock_now = datetime(2025, 1, 15, 14, 30, 0)
        assert is_digest_due(schedule, mock_now) is False

    def test_is_due_twice_daily(self):
        """Twice daily schedule matches at both times."""
        from services.notifier.digest_scheduler import is_digest_due

        schedule = {
            "frequency": "twice_daily",
            "hours": [9, 18],
            "minute": 0,
            "timezone": "Europe/Dublin",
        }

        assert is_digest_due(schedule, datetime(2025, 1, 15, 9, 0)) is True
        assert is_digest_due(schedule, datetime(2025, 1, 15, 18, 0)) is True
        assert is_digest_due(schedule, datetime(2025, 1, 15, 12, 0)) is False
