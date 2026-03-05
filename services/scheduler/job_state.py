"""
AGPARS Job State Machine

State transitions and validation for scraping jobs.
Follows the state machine defined in ARCHITECT.md:
    PENDING → RUNNING → SUCCESS
                     ↘ FAILED (retry_count < max) → PENDING
                     ↘ DEAD (retry_count >= max)
"""

from dataclasses import dataclass

from packages.core.config import get_settings
from packages.observability.logger import get_logger
from packages.storage.models import JobStatus

logger = get_logger(__name__)


# Valid state transitions
VALID_TRANSITIONS = {
    JobStatus.PENDING: [JobStatus.RUNNING],
    JobStatus.RUNNING: [JobStatus.SUCCESS, JobStatus.FAILED],
    JobStatus.FAILED: [JobStatus.PENDING, JobStatus.DEAD],
    JobStatus.SUCCESS: [],  # Terminal state
    JobStatus.DEAD: [],     # Terminal state
}


@dataclass
class StateTransitionResult:
    """Result of a state transition."""
    success: bool
    new_status: JobStatus
    error: str | None = None
    should_retry: bool = False


def is_valid_transition(current: JobStatus, target: JobStatus) -> bool:
    """Check if a state transition is valid."""
    return target in VALID_TRANSITIONS.get(current, [])


def transition_to_running(current: JobStatus) -> StateTransitionResult:
    """Transition job to RUNNING state."""
    if not is_valid_transition(current, JobStatus.RUNNING):
        return StateTransitionResult(
            success=False,
            new_status=current,
            error=f"Invalid transition: {current.value} → RUNNING",
        )
    return StateTransitionResult(success=True, new_status=JobStatus.RUNNING)


def transition_to_success(current: JobStatus) -> StateTransitionResult:
    """Transition job to SUCCESS state."""
    if not is_valid_transition(current, JobStatus.SUCCESS):
        return StateTransitionResult(
            success=False,
            new_status=current,
            error=f"Invalid transition: {current.value} → SUCCESS",
        )
    return StateTransitionResult(success=True, new_status=JobStatus.SUCCESS)


def transition_to_failed(
    current: JobStatus,
    retry_count: int,
    max_retries: int | None = None,
) -> StateTransitionResult:
    """
    Transition job to FAILED state.

    Args:
        current: Current job status
        retry_count: Current retry count
        max_retries: Maximum retries allowed (from config if None)

    Returns:
        StateTransitionResult with retry determination
    """
    if not is_valid_transition(current, JobStatus.FAILED):
        return StateTransitionResult(
            success=False,
            new_status=current,
            error=f"Invalid transition: {current.value} → FAILED",
        )

    settings = get_settings()
    max_retries = max_retries or settings.collector.max_retries

    if retry_count >= max_retries:
        # Move to DEAD state
        logger.warning(
            "Job exhausted retries, marking as DEAD",
            retry_count=retry_count,
            max_retries=max_retries,
        )
        return StateTransitionResult(
            success=True,
            new_status=JobStatus.DEAD,
            should_retry=False,
        )
    else:
        # Schedule for retry
        return StateTransitionResult(
            success=True,
            new_status=JobStatus.FAILED,
            should_retry=True,
        )


def is_terminal(status: JobStatus) -> bool:
    """Check if a status is terminal (no more transitions allowed)."""
    return len(VALID_TRANSITIONS.get(status, [])) == 0


def get_status_emoji(status: JobStatus) -> str:
    """Get emoji representation for status (for logging/display)."""
    return {
        JobStatus.PENDING: "⏳",
        JobStatus.RUNNING: "🔄",
        JobStatus.SUCCESS: "✅",
        JobStatus.FAILED: "⚠️",
        JobStatus.DEAD: "💀",
    }.get(status, "❓")
