"""
AGPARS Secrets Module

Secure handling of sensitive configuration values.
Provides validation and safe access patterns for secrets.
"""

import os
from pathlib import Path
from typing import Any

from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# SECRET RULES
# ═══════════════════════════════════════════════════════════════════════════════

# Required secrets that MUST be set in production
REQUIRED_SECRETS = [
    "TELEGRAM_BOT_TOKEN",
]

# Secrets that should never be logged or exposed
SENSITIVE_KEYS = [
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "KEY",
    "CREDENTIAL",
]

# Allowed secret sources (in order of precedence)
SECRET_SOURCES = [
    "environment",  # Environment variables
    "file",         # File-based secrets (e.g., Docker secrets)
]


# ═══════════════════════════════════════════════════════════════════════════════
# SECRET LOADER
# ═══════════════════════════════════════════════════════════════════════════════


def is_sensitive_key(key: str) -> bool:
    """Check if a key name indicates a sensitive value."""
    key_upper = key.upper()
    return any(sensitive in key_upper for sensitive in SENSITIVE_KEYS)


def mask_secret(value: str, visible_chars: int = 4) -> str:
    """Mask a secret value for safe logging."""
    if len(value) <= visible_chars:
        return "*" * len(value)
    return value[:visible_chars] + "*" * (len(value) - visible_chars)


def load_secret_from_file(secret_name: str, secrets_dir: str = "/run/secrets") -> str | None:
    """
    Load a secret from a file (Docker secrets pattern).

    Args:
        secret_name: Name of the secret
        secrets_dir: Directory containing secret files

    Returns:
        Secret value or None if not found
    """
    secret_path = Path(secrets_dir) / secret_name.lower()
    if secret_path.exists():
        try:
            return secret_path.read_text().strip()
        except Exception as e:
            logger.warning("Failed to read secret file", secret=secret_name, error=str(e))
    return None


def load_secret(secret_name: str, default: str | None = None) -> str | None:
    """
    Load a secret from available sources.

    Order of precedence:
    1. Environment variable
    2. File-based secret (Docker secrets)
    3. Default value

    Args:
        secret_name: Name of the secret (environment variable name)
        default: Default value if secret not found

    Returns:
        Secret value or default
    """
    # Try environment variable first
    value = os.environ.get(secret_name)
    if value:
        logger.debug("Secret loaded from environment", secret=secret_name)
        return value

    # Try file-based secret
    value = load_secret_from_file(secret_name)
    if value:
        logger.debug("Secret loaded from file", secret=secret_name)
        return value

    # Return default
    if default is not None:
        logger.debug("Using default for secret", secret=secret_name)
        return default

    return None


def validate_required_secrets(environment: str) -> list[str]:
    """
    Validate that all required secrets are present.

    Args:
        environment: Current deployment environment

    Returns:
        List of missing secret names

    Raises:
        ValueError: In production if any required secrets are missing
    """
    missing = []

    for secret_name in REQUIRED_SECRETS:
        value = load_secret(secret_name)
        if not value:
            missing.append(secret_name)

    if missing:
        if environment == "production":
            raise ValueError(f"Missing required secrets in production: {missing}")
        else:
            logger.warning(
                "Missing secrets (acceptable in non-production)",
                missing=missing,
                environment=environment,
            )

    return missing


def get_database_password() -> str:
    """Get database password from secure source."""
    return load_secret("POSTGRES_PASSWORD", default="agpars_dev") or ""


def get_redis_password() -> str | None:
    """Get Redis password from secure source."""
    return load_secret("REDIS_PASSWORD")


def get_telegram_token() -> str:
    """Get Telegram bot token from secure source."""
    return load_secret("TELEGRAM_BOT_TOKEN", default="") or ""


# ═══════════════════════════════════════════════════════════════════════════════
# SAFE DICT OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def sanitize_dict_for_logging(data: dict[str, Any]) -> dict[str, Any]:
    """
    Create a copy of a dict with sensitive values masked.

    Args:
        data: Dictionary potentially containing secrets

    Returns:
        Dictionary with sensitive values masked
    """
    sanitized = {}
    for key, value in data.items():
        if is_sensitive_key(key) and isinstance(value, str):
            sanitized[key] = mask_secret(value)
        elif isinstance(value, dict):
            sanitized[key] = sanitize_dict_for_logging(value)
        else:
            sanitized[key] = value
    return sanitized
