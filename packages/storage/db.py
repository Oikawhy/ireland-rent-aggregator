"""
AGPARS Database Module

SQLAlchemy engine and session management.
Provides connection pooling and schema-aware session factory.
"""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from packages.core.config import get_settings
from packages.observability.logger import get_logger

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# BASE MODEL
# ═══════════════════════════════════════════════════════════════════════════════

Base = declarative_base()


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

# PostgreSQL schemas as defined in ARCHITECT.md
SCHEMAS = {
    "raw": "Raw scraped data",
    "core": "Normalized entities",
    "ops": "Operational data (jobs, events, logs)",
    "pub": "Public-facing views (safe for bot)",
    "bot": "Bot configuration (workspaces, subscriptions)",
}


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE & SESSION FACTORY
# ═══════════════════════════════════════════════════════════════════════════════

_engine = None
_session_factory = None


def get_engine():
    """
    Get or create the SQLAlchemy engine.

    Uses connection pooling with sensible defaults for the rental pipeline.
    """
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            settings.database.url,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,  # Recycle connections after 30 minutes
            pool_pre_ping=True,  # Verify connections before use
            echo=settings.debug,
        )
        logger.info(
            "Database engine created",
            host=settings.database.host,
            database=settings.database.db,
        )
    return _engine


def get_session_factory() -> sessionmaker:
    """Get or create the session factory."""
    global _session_factory
    if _session_factory is None:
        engine = get_engine()
        _session_factory = sessionmaker(
            bind=engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _session_factory


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION CONTEXT MANAGERS
# ═══════════════════════════════════════════════════════════════════════════════


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Get a database session with automatic cleanup.

    Usage:
        with get_session() as session:
            session.query(...)
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_readonly_session() -> Generator[Session, None, None]:
    """
    Get a read-only database session.

    Automatically rolls back any changes (for safety).
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


# Alias for compatibility
get_session_context = get_session


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def create_schemas(session: Session) -> None:
    """
    Create all PostgreSQL schemas if they don't exist.

    Should be called during initial setup or migrations.
    """
    for schema_name in SCHEMAS:
        session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        logger.info("Schema ensured", schema=schema_name)
    session.commit()


def verify_connection() -> bool:
    """
    Verify database connection is working.

    Returns:
        True if connection is successful
    """
    try:
        with get_session() as session:
            session.execute(text("SELECT 1"))
            logger.info("Database connection verified")
            return True
    except Exception as e:
        logger.error("Database connection failed", error=str(e))
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════════════════════════════════


def dispose_engine() -> None:
    """Dispose of the engine and all connections."""
    global _engine, _session_factory
    if _engine is not None:
        _engine.dispose()
        _engine = None
        _session_factory = None
        logger.info("Database engine disposed")
