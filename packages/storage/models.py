"""
AGPARS ORM Models

SQLAlchemy models for all database tables.
Organized by schema as defined in ARCHITECT.md.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from packages.storage.db import Base

# ═══════════════════════════════════════════════════════════════════════════════
# ENUMS
# ═══════════════════════════════════════════════════════════════════════════════


class WorkspaceType(PyEnum):
    """Workspace types."""
    PERSONAL = "personal"
    GROUP = "group"


class DeliveryMode(PyEnum):
    """Delivery mode options."""
    INSTANT = "instant"
    DIGEST = "digest"
    PAUSED = "paused"


class PropertyType(PyEnum):
    """Property types."""
    APARTMENT = "apartment"
    HOUSE = "house"
    STUDIO = "studio"
    OTHER = "other"


class JobStatus(PyEnum):
    """Job status states."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    DEAD = "dead"


class EventStatus(PyEnum):
    """Event outbox status."""
    PENDING = "pending"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    FAILED = "failed"
    DEAD = "dead"


class EventType(PyEnum):
    """Event types."""
    NEW = "new"
    UPDATED = "updated"


class CircuitState(PyEnum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class ListingStatus(PyEnum):
    """Listing availability status."""
    ACTIVE = "active"
    REMOVED = "removed"


class UserRole(PyEnum):
    """Bot user roles."""
    UNAUTHORIZED = "unauthorized"
    REGULAR = "regular"
    ADMIN = "admin"


class AccessRequestStatus(PyEnum):
    """Access request status."""
    PENDING = "pending"
    APPROVED = "approved"
    DECLINED = "declined"


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA: bot — Bot Configuration
# ═══════════════════════════════════════════════════════════════════════════════


class Workspace(Base):
    """Telegram workspace (group or personal)."""

    __tablename__ = "workspaces"
    __table_args__ = {"schema": "bot"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    type: Mapped[WorkspaceType] = mapped_column(
        Enum(WorkspaceType, values_callable=lambda x: [e.value for e in x], name="workspacetype", schema="bot", create_type=False),
        nullable=False,
    )
    tg_chat_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False, index=True)
    title: Mapped[str | None] = mapped_column(String(255))
    timezone: Mapped[str] = mapped_column(String(50), default="Europe/Dublin")
    settings: Mapped[dict | None] = mapped_column(JSONB, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())
    owner_user_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("bot.users.id", ondelete="CASCADE")
    )

    # Relationships
    subscriptions: Mapped[list["Subscription"]] = relationship(back_populates="workspace")
    message_template: Mapped[Optional["MessageTemplate"]] = relationship(back_populates="workspace")


class BotUser(Base):
    """Bot user with role-based access control."""

    __tablename__ = "users"
    __table_args__ = {"schema": "bot"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tg_user_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False, index=True)
    tg_username: Mapped[str | None] = mapped_column(String(255))
    full_name: Mapped[str | None] = mapped_column(String(255))
    role: Mapped[UserRole] = mapped_column(
        Enum(UserRole, values_callable=lambda x: [e.value for e in x],
             name="userrole", schema="bot", create_type=False),
        default=UserRole.UNAUTHORIZED, nullable=False,
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    access_requests: Mapped[list["AccessRequest"]] = relationship(
        back_populates="user", foreign_keys="AccessRequest.user_id"
    )


class AccessRequest(Base):
    """Access request from unauthorized user."""

    __tablename__ = "access_requests"
    __table_args__ = {"schema": "bot"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("bot.users.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[AccessRequestStatus] = mapped_column(
        Enum(AccessRequestStatus, values_callable=lambda x: [e.value for e in x],
             name="accessrequeststatus", schema="bot", create_type=False),
        default=AccessRequestStatus.PENDING, nullable=False,
    )
    admin_message_id: Mapped[int | None] = mapped_column(BigInteger)
    reviewed_by_user_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("bot.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Relationships
    user: Mapped["BotUser"] = relationship(back_populates="access_requests", foreign_keys=[user_id])


class Subscription(Base):
    """Workspace subscription filters."""

    __tablename__ = "subscriptions"
    __table_args__ = {"schema": "bot"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    workspace_id: Mapped[int] = mapped_column(Integer, ForeignKey("bot.workspaces.id"), nullable=False)
    name: Mapped[str | None] = mapped_column(String(100))
    filters: Mapped[dict] = mapped_column(JSONB, default=dict)
    delivery_mode: Mapped[DeliveryMode] = mapped_column(
        Enum(DeliveryMode, values_callable=lambda x: [e.value for e in x], name="deliverymode", schema="bot", create_type=False),
        default=DeliveryMode.INSTANT,
    )
    digest_schedule: Mapped[dict | None] = mapped_column(JSONB)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    workspace: Mapped["Workspace"] = relationship(back_populates="subscriptions")


class MessageTemplate(Base):
    """Workspace message template."""

    __tablename__ = "message_templates"
    __table_args__ = {"schema": "bot"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    workspace_id: Mapped[int] = mapped_column(Integer, ForeignKey("bot.workspaces.id"), unique=True)
    template: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    workspace: Mapped["Workspace"] = relationship(back_populates="message_template")


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA: core — Normalized Entities
# ═══════════════════════════════════════════════════════════════════════════════


class City(Base):
    """Master city list (95 cities)."""

    __tablename__ = "cities"
    __table_args__ = {"schema": "core"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    county: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    population: Mapped[int | None] = mapped_column(Integer)
    synonyms: Mapped[list[str] | None] = mapped_column(ARRAY(Text))


class ListingNormalized(Base):
    """Normalized listing data."""

    __tablename__ = "listings_normalized"
    __table_args__ = (
        Index("ix_listings_norm_city_id", "city_id"),
        Index("ix_listings_norm_updated_at", "updated_at"),
        {"schema": "core"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_id: Mapped[int] = mapped_column(Integer, ForeignKey("raw.listings_raw.id"), unique=True)
    price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    beds: Mapped[int | None] = mapped_column(Integer)
    baths: Mapped[int | None] = mapped_column(Integer)
    property_type: Mapped[PropertyType | None] = mapped_column(
        Enum(PropertyType, values_callable=lambda x: [e.value for e in x], name="propertytype", schema="core", create_type=False),
    )
    furnished: Mapped[bool | None] = mapped_column(Boolean)
    lease_length_months: Mapped[int | None] = mapped_column(Integer)
    lease_length_unknown: Mapped[bool] = mapped_column(Boolean, default=False)
    city_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("core.cities.id"))
    county: Mapped[str | None] = mapped_column(String(100))
    area_text: Mapped[str | None] = mapped_column(String(500))
    status: Mapped[ListingStatus] = mapped_column(
        Enum(ListingStatus, values_callable=lambda x: [e.value for e in x], name="listingstatus", schema="core", create_type=False),
        default=ListingStatus.ACTIVE,
    )
    search_vector: Mapped[str | None] = mapped_column(TSVECTOR)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    raw_listing: Mapped["ListingRaw"] = relationship(back_populates="normalized")


class ListingLink(Base):
    """Cross-source duplicate links."""

    __tablename__ = "listing_links"
    __table_args__ = {"schema": "core"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_id_a: Mapped[int] = mapped_column(Integer, ForeignKey("raw.listings_raw.id"), nullable=False)
    raw_id_b: Mapped[int] = mapped_column(Integer, ForeignKey("raw.listings_raw.id"), nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA: raw — Raw Scraped Data
# ═══════════════════════════════════════════════════════════════════════════════


class ListingRaw(Base):
    """Raw scraped listing data."""

    __tablename__ = "listings_raw"
    __table_args__ = (
        Index("ix_listings_raw_source_listing", "source", "source_listing_id", unique=True),
        {"schema": "raw"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    source_listing_id: Mapped[str] = mapped_column(String(100), nullable=False)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    raw_payload: Mapped[dict | None] = mapped_column(JSONB)
    first_photo_url: Mapped[str | None] = mapped_column(String(2048))
    first_seen: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Parsed text fields (extracted by adapters, used by normalizer)
    title: Mapped[str | None] = mapped_column(String(500))
    price_text: Mapped[str | None] = mapped_column(String(100))
    beds_text: Mapped[str | None] = mapped_column(String(50))
    baths_text: Mapped[str | None] = mapped_column(String(50))
    property_type_text: Mapped[str | None] = mapped_column(String(100))
    location_text: Mapped[str | None] = mapped_column(String(500))
    description: Mapped[str | None] = mapped_column(Text)

    # Relationships
    normalized: Mapped[Optional["ListingNormalized"]] = relationship(back_populates="raw_listing")


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA: ops — Operational Data
# ═══════════════════════════════════════════════════════════════════════════════


class JobLog(Base):
    """Scraping job log."""

    __tablename__ = "job_log"
    __table_args__ = (
        Index("ix_job_log_source_city", "source", "city_id"),
        Index("ix_job_log_status", "status"),
        {"schema": "ops"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    city_id: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, values_callable=lambda x: [e.value for e in x], name="jobstatus", schema="ops", create_type=False),
        default=JobStatus.PENDING,
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    listings_found: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())


class EventOutbox(Base):
    """Event outbox for delivery."""

    __tablename__ = "event_outbox"
    __table_args__ = (
        Index("ix_event_outbox_status", "status"),
        Index("ix_event_outbox_workspace", "workspace_id"),
        {"schema": "ops"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    workspace_id: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[EventType] = mapped_column(
        Enum(EventType, values_callable=lambda x: [e.value for e in x], name="eventtype", schema="ops", create_type=False),
        nullable=False,
    )
    listing_raw_id: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    status: Mapped[EventStatus] = mapped_column(
        Enum(EventStatus, values_callable=lambda x: [e.value for e in x], name="eventstatus", schema="ops", create_type=False),
        default=EventStatus.PENDING,
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime)


class DeliveryLog(Base):
    """Telegram delivery log for idempotency."""

    __tablename__ = "delivery_log"
    __table_args__ = (
        Index("ix_delivery_log_workspace_event", "workspace_id", "event_id", unique=True),
        {"schema": "ops"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    workspace_id: Mapped[int] = mapped_column(Integer, nullable=False)
    event_id: Mapped[int] = mapped_column(Integer, nullable=False)
    telegram_message_id: Mapped[int | None] = mapped_column(BigInteger)
    sent_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())


class SourceCircuitBreaker(Base):
    """Circuit breaker state per source."""

    __tablename__ = "source_circuit_breakers"
    __table_args__ = {"schema": "ops"}

    source: Mapped[str] = mapped_column(String(50), primary_key=True)
    state: Mapped[CircuitState] = mapped_column(
        Enum(CircuitState, values_callable=lambda x: [e.value for e in x], name="circuitstate", schema="ops", create_type=False),
        default=CircuitState.CLOSED
    )
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime)
    recovery_at: Mapped[datetime | None] = mapped_column(DateTime)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


class Watermark(Base):
    """Watermarks for incremental sync operations."""

    __tablename__ = "watermarks"
    __table_args__ = {"schema": "ops"}

    key: Mapped[str] = mapped_column(String(100), primary_key=True)
    value: Mapped[str | None] = mapped_column(String(100))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA: pub — Public-Facing Views
# ═══════════════════════════════════════════════════════════════════════════════


class PublicListing(Base):
    """Public listing view (safe fields only)."""

    __tablename__ = "public_listings"
    __table_args__ = (
        Index("ix_public_listings_updated_at", "updated_at"),
        Index("ix_public_listings_county", "county"),
        Index("ix_public_listings_city", "city"),
        Index("ix_public_listings_price", "price"),
        Index("ix_public_listings_beds", "beds"),
        {"schema": "pub"},
    )

    listing_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    beds: Mapped[int | None] = mapped_column(Integer)
    baths: Mapped[int | None] = mapped_column(Integer)
    property_type: Mapped[str | None] = mapped_column(String(50))
    county: Mapped[str | None] = mapped_column(String(100))
    city: Mapped[str | None] = mapped_column(String(255))
    area_text: Mapped[str | None] = mapped_column(String(500))
    first_photo_url: Mapped[str | None] = mapped_column(String(2048))
    published_at: Mapped[datetime] = mapped_column(DateTime)
    updated_at: Mapped[datetime] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String(20), default="active")
