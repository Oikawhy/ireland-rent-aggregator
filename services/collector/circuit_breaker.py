"""
AGPARS Circuit Breaker Module

Circuit breaker pattern for source-level failure handling.
"""

from datetime import datetime, timedelta

from packages.observability.logger import get_logger
from packages.storage.db import get_session
from packages.storage.models import CircuitState, SourceCircuitBreaker

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════


# Default circuit breaker settings (can be overridden per-source)
DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_RECOVERY_TIMEOUT_MINUTES = 30
DEFAULT_HALF_OPEN_REQUESTS = 3

# Aliases for test compatibility
FAILURE_THRESHOLD = DEFAULT_FAILURE_THRESHOLD
RECOVERY_TIMEOUT_SECONDS = DEFAULT_RECOVERY_TIMEOUT_MINUTES * 60

# Re-export CircuitState for convenience
CircuitState = CircuitState


# ═══════════════════════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════


def check_circuit(source: str) -> str:
    """
    Check the current circuit state for a source.

    Returns:
        'closed', 'open', or 'half_open'
    """
    with get_session() as session:
        circuit = session.get(SourceCircuitBreaker, source)

        if not circuit:
            # No record = closed (healthy)
            return "closed"

        state = circuit.state

        # Check if OPEN circuit should transition to HALF_OPEN
        if state == CircuitState.OPEN and circuit.recovery_at:  # noqa: SIM102
            if datetime.utcnow() >= circuit.recovery_at:
                # Transition to HALF_OPEN
                circuit.state = CircuitState.HALF_OPEN
                circuit.failure_count = 0
                session.commit()
                logger.info("Circuit transitioned to HALF_OPEN", source=source)
                return "half_open"

        return state.value


def record_success(source: str) -> None:
    """Record a successful request for a source."""
    with get_session() as session:
        circuit = session.get(SourceCircuitBreaker, source)

        if not circuit:
            # No circuit record = all good
            return

        if circuit.state == CircuitState.HALF_OPEN:
            # Success in half-open = close circuit
            circuit.state = CircuitState.CLOSED
            circuit.failure_count = 0
            circuit.last_failure_at = None
            circuit.recovery_at = None
            logger.info("Circuit CLOSED after successful half-open test", source=source)

        elif circuit.state == CircuitState.CLOSED:
            # Reset failure count on success
            if circuit.failure_count > 0:
                circuit.failure_count = 0

        session.commit()


def record_failure(source: str, reason: str = "") -> None:
    """Record a failed request for a source."""
    with get_session() as session:
        circuit = session.get(SourceCircuitBreaker, source)

        if not circuit:
            # Create new circuit record
            circuit = SourceCircuitBreaker(
                source=source,
                state=CircuitState.CLOSED,
                failure_count=1,
                last_failure_at=datetime.utcnow(),
            )
            session.add(circuit)
        else:
            circuit.failure_count += 1
            circuit.last_failure_at = datetime.utcnow()

            # Check if should trip to OPEN
            if circuit.state == CircuitState.CLOSED:
                if circuit.failure_count >= DEFAULT_FAILURE_THRESHOLD:
                    circuit.state = CircuitState.OPEN
                    circuit.recovery_at = datetime.utcnow() + timedelta(
                        minutes=DEFAULT_RECOVERY_TIMEOUT_MINUTES
                    )
                    logger.warning(
                        "Circuit OPEN",
                        source=source,
                        failures=circuit.failure_count,
                        reason=reason,
                        recovery_at=circuit.recovery_at.isoformat(),
                    )

            elif circuit.state == CircuitState.HALF_OPEN:
                # Failure in half-open = back to OPEN
                circuit.state = CircuitState.OPEN
                circuit.recovery_at = datetime.utcnow() + timedelta(
                    minutes=DEFAULT_RECOVERY_TIMEOUT_MINUTES
                )
                logger.warning("Circuit re-OPENED after half-open failure", source=source)

        session.commit()


def force_open(source: str, reason: str = "manual") -> None:
    """Force a circuit to OPEN state."""
    with get_session() as session:
        circuit = session.get(SourceCircuitBreaker, source)

        if not circuit:
            circuit = SourceCircuitBreaker(
                source=source,
                state=CircuitState.OPEN,
                failure_count=0,
                last_failure_at=datetime.utcnow(),
                recovery_at=datetime.utcnow() + timedelta(minutes=DEFAULT_RECOVERY_TIMEOUT_MINUTES),
            )
            session.add(circuit)
        else:
            circuit.state = CircuitState.OPEN
            circuit.recovery_at = datetime.utcnow() + timedelta(
                minutes=DEFAULT_RECOVERY_TIMEOUT_MINUTES
            )

        session.commit()
        logger.warning("Circuit force-OPENED", source=source, reason=reason)


def force_close(source: str) -> None:
    """Force a circuit to CLOSED state."""
    with get_session() as session:
        circuit = session.get(SourceCircuitBreaker, source)

        if circuit:
            circuit.state = CircuitState.CLOSED
            circuit.failure_count = 0
            circuit.last_failure_at = None
            circuit.recovery_at = None
            session.commit()
            logger.info("Circuit force-CLOSED", source=source)


def reset_circuit(source: str) -> None:
    """Reset circuit breaker to initial state."""
    force_close(source)


# ═══════════════════════════════════════════════════════════════════════════════
# STATUS
# ═══════════════════════════════════════════════════════════════════════════════


def get_circuit_status(source: str) -> dict | None:
    """Get full circuit breaker status for a source."""
    with get_session() as session:
        circuit = session.get(SourceCircuitBreaker, source)

        if not circuit:
            return {
                "source": source,
                "state": "closed",
                "failure_count": 0,
                "last_failure_at": None,
                "recovery_at": None,
            }

        return {
            "source": circuit.source,
            "state": circuit.state.value,
            "failure_count": circuit.failure_count,
            "last_failure_at": circuit.last_failure_at,
            "recovery_at": circuit.recovery_at,
            "updated_at": circuit.updated_at,
        }


def get_all_circuit_statuses() -> list[dict]:
    """Get status of all circuit breakers."""
    from sqlalchemy import select

    with get_session() as session:
        result = session.execute(select(SourceCircuitBreaker))
        return [
            {
                "source": c.source,
                "state": c.state.value,
                "failure_count": c.failure_count,
                "last_failure_at": c.last_failure_at,
                "recovery_at": c.recovery_at,
            }
            for c in result.scalars().all()
        ]


def is_source_available(source: str) -> bool:
    """Check if a source is available for scraping."""
    state = check_circuit(source)
    return state in ("closed", "half_open")
