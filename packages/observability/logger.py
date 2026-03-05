"""
AGPARS Structured Logging Module

Provides consistent, structured logging using structlog.
Supports JSON output for production and pretty console output for development.
"""

import logging
import sys
from typing import Any

import structlog
from structlog.types import Processor

from packages.core.config import get_settings


def add_app_context(
    logger: logging.Logger, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Add application context to all log entries."""
    settings = get_settings()
    event_dict["app"] = settings.app_name
    event_dict["environment"] = settings.environment
    return event_dict


def configure_logging() -> None:
    """
    Configure structlog with appropriate processors based on environment.

    Call this once at application startup.
    """
    settings = get_settings()

    # Shared processors
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        add_app_context,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if settings.observability.log_format == "json":
        # Production: JSON output
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Development: Pretty console output
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.observability.log_level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Also configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.observability.log_level),
    )


def get_logger(name: str | None = None) -> structlog.BoundLogger:
    """
    Get a structured logger instance.

    Args:
        name: Optional logger name (usually __name__)

    Returns:
        Configured structlog BoundLogger
    """
    return structlog.get_logger(name)


# Convenience function to bind context
def bind_context(**kwargs: Any) -> None:
    """
    Bind context variables to all subsequent log calls in the current context.

    Example:
        bind_context(job_id="123", source="daft.ie")
        logger.info("Processing job")  # Will include job_id and source
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear all bound context variables."""
    structlog.contextvars.clear_contextvars()


# Pre-configured loggers for common components
class Loggers:
    """Pre-configured loggers for application components."""

    @staticmethod
    def collector() -> structlog.BoundLogger:
        return get_logger("collector")

    @staticmethod
    def normalizer() -> structlog.BoundLogger:
        return get_logger("normalizer")

    @staticmethod
    def publisher() -> structlog.BoundLogger:
        return get_logger("publisher")

    @staticmethod
    def notifier() -> structlog.BoundLogger:
        return get_logger("notifier")

    @staticmethod
    def bot() -> structlog.BoundLogger:
        return get_logger("bot")

    @staticmethod
    def scheduler() -> structlog.BoundLogger:
        return get_logger("scheduler")
