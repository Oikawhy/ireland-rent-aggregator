"""
Alembic Migration Environment

Configures SQLAlchemy connection and metadata for migrations.
"""

import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from packages.storage.db import Base
from packages.storage.models import *  # noqa: F401, F403 - Import all models for metadata
from packages.core.config import get_settings

# Alembic Config object
config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Add model's MetaData object for 'autogenerate' support
target_metadata = Base.metadata


def get_url() -> str:
    """Get database URL from environment or config."""
    # Try environment variable first (for CI/CD)
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    # Try app settings
    try:
        settings = get_settings()
        return settings.database.url
    except Exception:
        # Fallback to alembic.ini value
        return config.get_main_option("sqlalchemy.url") or ""


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.

    Configures context with just a URL and not an Engine.
    Emits migration SQL to stdout.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,  # Important for multi-schema support
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode.

    Creates an Engine and associates a connection with the context.
    """
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,  # Important for multi-schema support
            compare_type=True,  # Detect column type changes
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
