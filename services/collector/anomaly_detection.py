"""
AGPARS Anomaly Detection Module

Automatic detection of scraping anomalies to trigger circuit breakers.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# ANOMALY TYPE ENUM
# ═══════════════════════════════════════════════════════════════════════════════


class AnomalyType(Enum):
    """Types of detectable anomalies."""

    VOLUME_DROP = "volume_drop"
    PARSE_FAILURE_SPIKE = "parse_failure_spike"
    RESPONSE_TIME_SPIKE = "response_time_spike"


# ═══════════════════════════════════════════════════════════════════════════════
# THRESHOLDS
# ═══════════════════════════════════════════════════════════════════════════════


# Threshold constants for test compatibility
VOLUME_DROP_THRESHOLD = 0.20  # 20% of average = volume drop
PARSE_FAILURE_THRESHOLD = 0.50  # 50% parse failure = spike
RESPONSE_TIME_MULTIPLIER = 3.0  # 3x average = response time spike


@dataclass
class AnomalyThresholds:
    """Configurable anomaly detection thresholds."""

    # Volume drop: < X% of rolling average
    volume_drop_percent: float = VOLUME_DROP_THRESHOLD
    volume_rolling_days: int = 7

    # Parse failure spike: > X% of listings fail
    parse_failure_percent: float = PARSE_FAILURE_THRESHOLD

    # Response time spike: > Xx average
    response_time_multiplier: float = RESPONSE_TIME_MULTIPLIER

    # Minimum samples for reliable detection
    min_samples: int = 5


DEFAULT_THRESHOLDS = AnomalyThresholds()


# ═══════════════════════════════════════════════════════════════════════════════
# ANOMALY TYPES
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""

    source: str
    anomaly_type: str | None  # volume_drop, parse_failure, response_time
    severity: str  # warning, critical
    details: dict
    should_open_circuit: bool = False


# ═══════════════════════════════════════════════════════════════════════════════
# ANOMALY DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════


class AnomalyDetector:
    """
    Detects scraping anomalies based on historical data.

    Types of anomalies:
    - Volume drop: significantly fewer listings than expected
    - Parse failure spike: high rate of parsing failures
    - Response time spike: much slower than usual
    """

    def __init__(self, thresholds: AnomalyThresholds = DEFAULT_THRESHOLDS):
        self.thresholds = thresholds
        self._history: dict[str, list[dict]] = {}

    def record_scrape(
        self,
        source: str,
        listings_found: int,
        parse_failures: int,
        duration_seconds: float,
    ) -> None:
        """Record metrics from a scrape job."""
        if source not in self._history:
            self._history[source] = []

        self._history[source].append({
            "timestamp": datetime.utcnow(),
            "listings_found": listings_found,
            "parse_failures": parse_failures,
            "duration_seconds": duration_seconds,
        })

        # Keep only last 7 days
        cutoff = datetime.utcnow() - timedelta(days=self.thresholds.volume_rolling_days)
        self._history[source] = [
            h for h in self._history[source]
            if h["timestamp"] > cutoff
        ]

    def detect_anomalies(
        self,
        source: str,
        current_listings: int,
        current_failures: int,
        current_duration: float,
    ) -> AnomalyResult | None:
        """
        Check for anomalies in current scrape results.

        Returns:
            AnomalyResult if anomaly detected, None otherwise
        """
        history = self._history.get(source, [])

        if len(history) < self.thresholds.min_samples:
            # Not enough data
            return None

        # Calculate averages
        avg_listings = sum(h["listings_found"] for h in history) / len(history)
        avg_duration = sum(h["duration_seconds"] for h in history) / len(history)

        # Check volume drop
        if avg_listings > 0:
            volume_ratio = current_listings / avg_listings
            if volume_ratio < self.thresholds.volume_drop_percent:
                logger.warning(
                    "Volume drop detected",
                    source=source,
                    current=current_listings,
                    average=avg_listings,
                    ratio=volume_ratio,
                )
                return AnomalyResult(
                    source=source,
                    anomaly_type="volume_drop",
                    severity="critical",
                    details={
                        "current": current_listings,
                        "average": avg_listings,
                        "ratio": volume_ratio,
                        "threshold": self.thresholds.volume_drop_percent,
                    },
                    should_open_circuit=True,
                )

        # Check parse failure spike
        total_attempted = current_listings + current_failures
        if total_attempted > 0:
            failure_rate = current_failures / total_attempted
            if failure_rate > self.thresholds.parse_failure_percent:
                logger.warning(
                    "Parse failure spike detected",
                    source=source,
                    failures=current_failures,
                    total=total_attempted,
                    rate=failure_rate,
                )
                return AnomalyResult(
                    source=source,
                    anomaly_type="parse_failure",
                    severity="critical",
                    details={
                        "failures": current_failures,
                        "total": total_attempted,
                        "rate": failure_rate,
                        "threshold": self.thresholds.parse_failure_percent,
                    },
                    should_open_circuit=True,
                )

        # Check response time spike
        if avg_duration > 0:
            time_ratio = current_duration / avg_duration
            if time_ratio > self.thresholds.response_time_multiplier:
                logger.warning(
                    "Response time spike detected",
                    source=source,
                    current=current_duration,
                    average=avg_duration,
                    ratio=time_ratio,
                )
                return AnomalyResult(
                    source=source,
                    anomaly_type="response_time",
                    severity="warning",
                    details={
                        "current": current_duration,
                        "average": avg_duration,
                        "ratio": time_ratio,
                        "threshold": self.thresholds.response_time_multiplier,
                    },
                    should_open_circuit=False,  # Warning only
                )

        return None

    def get_source_stats(self, source: str) -> dict:
        """Get statistics for a source."""
        history = self._history.get(source, [])

        if not history:
            return {
                "source": source,
                "samples": 0,
                "avg_listings": 0,
                "avg_duration": 0,
            }

        return {
            "source": source,
            "samples": len(history),
            "avg_listings": sum(h["listings_found"] for h in history) / len(history),
            "avg_duration": sum(h["duration_seconds"] for h in history) / len(history),
            "avg_failures": sum(h["parse_failures"] for h in history) / len(history),
            "min_listings": min(h["listings_found"] for h in history),
            "max_listings": max(h["listings_found"] for h in history),
        }

    def clear_history(self, source: str | None = None) -> None:
        """Clear history for a source or all sources."""
        if source:
            if source in self._history:
                del self._history[source]
        else:
            self._history.clear()


# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL INSTANCE
# ═══════════════════════════════════════════════════════════════════════════════


_detector: AnomalyDetector | None = None


def get_detector() -> AnomalyDetector:
    """Get the global anomaly detector instance."""
    global _detector
    if _detector is None:
        _detector = AnomalyDetector()
    return _detector


def detect_and_handle_anomaly(
    source: str,
    listings_found: int,
    parse_failures: int,
    duration_seconds: float,
) -> AnomalyResult | None:
    """
    Detect anomalies and handle circuit breaker if needed.

    Returns:
        AnomalyResult if anomaly detected
    """
    from services.collector.circuit_breaker import force_open

    detector = get_detector()

    # Record metrics
    detector.record_scrape(source, listings_found, parse_failures, duration_seconds)

    # Check for anomalies
    result = detector.detect_anomalies(
        source, listings_found, parse_failures, duration_seconds
    )

    if result and result.should_open_circuit:
        force_open(source, reason=f"anomaly:{result.anomaly_type}")

    return result
