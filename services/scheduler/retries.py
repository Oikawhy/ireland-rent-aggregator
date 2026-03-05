"""
AGPARS Retry Module

Exponential backoff, dead-letter handling, and source pause logic.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

from packages.core.config import get_settings
from packages.observability.logger import get_logger
from packages.observability.metrics import set_circuit_breaker_state
from packages.storage.db import get_session
from packages.storage.models import CircuitState, SourceCircuitBreaker

logger = get_logger(__name__)


# Backoff intervals in seconds (1min, 5min, 15min)
BACKOFF_INTERVALS = [60, 300, 900]


@dataclass
class RetryDecision:
    """Decision about whether and when to retry."""
    should_retry: bool
    delay_seconds: int
    reason: str


def calculate_backoff(retry_count: int) -> int:
    """
    Calculate exponential backoff delay.

    Args:
        retry_count: Number of previous retries (0-based)

    Returns:
        Delay in seconds before next retry
    """
    if retry_count >= len(BACKOFF_INTERVALS):
        return BACKOFF_INTERVALS[-1]
    return BACKOFF_INTERVALS[retry_count]


def should_retry_job(retry_count: int, max_retries: int | None = None) -> RetryDecision:
    """
    Determine if a job should be retried.

    Args:
        retry_count: Current retry count
        max_retries: Maximum allowed retries

    Returns:
        RetryDecision with delay if should retry
    """
    settings = get_settings()
    max_retries = max_retries or settings.collector.max_retries

    if retry_count >= max_retries:
        return RetryDecision(
            should_retry=False,
            delay_seconds=0,
            reason=f"Exceeded max retries ({max_retries})",
        )

    delay = calculate_backoff(retry_count)
    return RetryDecision(
        should_retry=True,
        delay_seconds=delay,
        reason=f"Retry {retry_count + 1}/{max_retries} after {delay}s",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# CIRCUIT BREAKER
# ═══════════════════════════════════════════════════════════════════════════════

# Thresholds for circuit breaker
FAILURE_THRESHOLD = 5  # Failures before opening circuit
RECOVERY_TIMEOUT = 300  # Seconds before half-open attempt


def get_circuit_breaker(source: str) -> SourceCircuitBreaker | None:
    """Get circuit breaker state for a source."""
    with get_session() as session:
        return session.get(SourceCircuitBreaker, source)


def is_source_paused(source: str) -> bool:
    """
    Check if a source is paused (circuit open).

    Returns:
        True if the source should not be scraped
    """
    breaker = get_circuit_breaker(source)
    if breaker is None:
        return False

    if breaker.state == CircuitState.OPEN:
        # Check if recovery time has passed (allow half-open)
        if breaker.recovery_at and datetime.utcnow() >= breaker.recovery_at:
            _transition_to_half_open(source)
            return False
        return True

    return False


def record_source_failure(source: str) -> None:
    """Record a failure for a source, potentially opening circuit."""
    with get_session() as session:
        breaker = session.get(SourceCircuitBreaker, source)

        if breaker is None:
            breaker = SourceCircuitBreaker(source=source)
            session.add(breaker)

        breaker.failure_count += 1
        breaker.last_failure_at = datetime.utcnow()

        if breaker.failure_count >= FAILURE_THRESHOLD:
            breaker.state = CircuitState.OPEN
            breaker.recovery_at = datetime.utcnow() + timedelta(seconds=RECOVERY_TIMEOUT)
            logger.warning(
                "Circuit breaker OPENED",
                source=source,
                failure_count=breaker.failure_count,
                recovery_at=breaker.recovery_at.isoformat(),
            )
            set_circuit_breaker_state(source, 2)  # OPEN = 2

        session.commit()


def record_source_success(source: str) -> None:
    """Record a success for a source, potentially closing circuit."""
    with get_session() as session:
        breaker = session.get(SourceCircuitBreaker, source)

        if breaker is None:
            return  # No breaker, nothing to reset

        if breaker.state == CircuitState.HALF_OPEN:
            # Success in half-open means we can close
            breaker.state = CircuitState.CLOSED
            breaker.failure_count = 0
            logger.info("Circuit breaker CLOSED after successful half-open attempt", source=source)
            set_circuit_breaker_state(source, 0)  # CLOSED = 0
        elif breaker.state == CircuitState.CLOSED:
            # Reset failure count on success
            breaker.failure_count = 0

        session.commit()


def _transition_to_half_open(source: str) -> None:
    """Internal: transition circuit to half-open state."""
    with get_session() as session:
        breaker = session.get(SourceCircuitBreaker, source)
        if breaker and breaker.state == CircuitState.OPEN:
            breaker.state = CircuitState.HALF_OPEN
            logger.info("Circuit breaker HALF-OPEN, attempting recovery", source=source)
            set_circuit_breaker_state(source, 1)  # HALF_OPEN = 1
        session.commit()


def reset_circuit_breaker(source: str) -> None:
    """Manually reset a circuit breaker."""
    with get_session() as session:
        breaker = session.get(SourceCircuitBreaker, source)
        if breaker:
            breaker.state = CircuitState.CLOSED
            breaker.failure_count = 0
            breaker.last_failure_at = None
            breaker.recovery_at = None
            logger.info("Circuit breaker manually reset", source=source)
            set_circuit_breaker_state(source, 0)
        session.commit()


def get_all_circuit_states() -> dict[str, str]:
    """Get circuit breaker state for all sources."""
    with get_session() as session:
        from sqlalchemy import select
        result = session.execute(select(SourceCircuitBreaker))
        return {cb.source: cb.state.value for cb in result.scalars().all()}
