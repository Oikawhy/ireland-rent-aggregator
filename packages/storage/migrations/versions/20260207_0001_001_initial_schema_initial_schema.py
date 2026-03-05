"""Initial schema setup with all tables

Revision ID: 001_initial_schema
Revises:
Create Date: 2026-02-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ═══════════════════════════════════════════════════════════════════════════════
    # CREATE SCHEMAS (T027.1)
    # ═══════════════════════════════════════════════════════════════════════════════
    op.execute("CREATE SCHEMA IF NOT EXISTS raw")
    op.execute("CREATE SCHEMA IF NOT EXISTS core")
    op.execute("CREATE SCHEMA IF NOT EXISTS ops")
    op.execute("CREATE SCHEMA IF NOT EXISTS pub")
    op.execute("CREATE SCHEMA IF NOT EXISTS bot")

    # ═══════════════════════════════════════════════════════════════════════════════
    # SCHEMA: bot — Bot Configuration
    # ═══════════════════════════════════════════════════════════════════════════════

    # Workspaces
    op.create_table(
        "workspaces",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("type", sa.Enum("personal", "group", name="workspacetype", schema="bot"), nullable=False),
        sa.Column("tg_chat_id", sa.BigInteger(), unique=True, nullable=False),
        sa.Column("title", sa.String(255)),
        sa.Column("timezone", sa.String(50), server_default="Europe/Dublin"),
        sa.Column("settings", postgresql.JSONB(), server_default="{}"),
        sa.Column("is_active", sa.Boolean(), server_default="true"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        schema="bot",
    )
    op.create_index("ix_bot_workspaces_tg_chat_id", "workspaces", ["tg_chat_id"], schema="bot")

    # Workspace Admins
    op.create_table(
        "workspace_admins",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("workspace_id", sa.Integer(), sa.ForeignKey("bot.workspaces.id", ondelete="CASCADE"), nullable=False),
        sa.Column("tg_user_id", sa.BigInteger(), nullable=False),
        sa.Column("added_at", sa.DateTime(), server_default=sa.func.now()),
        schema="bot",
    )
    op.create_index("ix_bot_workspace_admins_tg_user_id", "workspace_admins", ["tg_user_id"], schema="bot")

    # Subscriptions
    op.create_table(
        "subscriptions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("workspace_id", sa.Integer(), sa.ForeignKey("bot.workspaces.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(100)),
        sa.Column("filters", postgresql.JSONB(), server_default="{}"),
        sa.Column("delivery_mode", sa.Enum("instant", "digest", "paused", name="deliverymode", schema="bot"), server_default="instant"),
        sa.Column("digest_schedule", postgresql.JSONB()),
        sa.Column("is_enabled", sa.Boolean(), server_default="true"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        schema="bot",
    )

    # Message Templates
    op.create_table(
        "message_templates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("workspace_id", sa.Integer(), sa.ForeignKey("bot.workspaces.id", ondelete="CASCADE"), unique=True),
        sa.Column("template", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        schema="bot",
    )

    # ═══════════════════════════════════════════════════════════════════════════════
    # SCHEMA: core — Normalized Entities
    # ═══════════════════════════════════════════════════════════════════════════════

    # Cities (master list)
    op.create_table(
        "cities",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(100), nullable=False, unique=True),
        sa.Column("county", sa.String(100), nullable=False),
        sa.Column("population", sa.Integer()),
        sa.Column("synonyms", postgresql.ARRAY(sa.Text())),
        schema="core",
    )
    op.create_index("ix_core_cities_county", "cities", ["county"], schema="core")

    # ═══════════════════════════════════════════════════════════════════════════════
    # SCHEMA: raw — Raw Scraped Data
    # ═══════════════════════════════════════════════════════════════════════════════

    # Raw Listings
    op.create_table(
        "listings_raw",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("source_listing_id", sa.String(100), nullable=False),
        sa.Column("url", sa.String(2048), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB()),
        sa.Column("first_photo_url", sa.String(2048)),
        sa.Column("first_seen", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("last_seen", sa.DateTime(), server_default=sa.func.now()),
        schema="raw",
    )
    op.create_index("ix_raw_listings_source", "listings_raw", ["source"], schema="raw")
    op.create_index("ix_raw_listings_source_listing", "listings_raw", ["source", "source_listing_id"], unique=True, schema="raw")

    # ═══════════════════════════════════════════════════════════════════════════════
    # SCHEMA: core — Normalized Listings (depends on raw.listings_raw)
    # ═══════════════════════════════════════════════════════════════════════════════

    # Normalized Listings
    op.create_table(
        "listings_normalized",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("raw_id", sa.Integer(), sa.ForeignKey("raw.listings_raw.id", ondelete="CASCADE"), unique=True),
        sa.Column("price", sa.Numeric(12, 2)),
        sa.Column("beds", sa.Integer()),
        sa.Column("baths", sa.Integer()),
        sa.Column("property_type", sa.Enum("apartment", "house", "studio", "other", name="propertytype", schema="core")),
        sa.Column("furnished", sa.Boolean()),
        sa.Column("lease_length_months", sa.Integer()),
        sa.Column("lease_length_unknown", sa.Boolean(), server_default="false"),
        sa.Column("city_id", sa.Integer(), sa.ForeignKey("core.cities.id")),
        sa.Column("county", sa.String(100)),
        sa.Column("area_text", sa.String(500)),
        sa.Column("status", sa.Enum("active", "removed", name="listingstatus", schema="core"), server_default="active"),
        sa.Column("search_vector", postgresql.TSVECTOR()),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        schema="core",
    )
    op.create_index("ix_core_listings_norm_city_id", "listings_normalized", ["city_id"], schema="core")
    op.create_index("ix_core_listings_norm_updated_at", "listings_normalized", ["updated_at"], schema="core")

    # Listing Links (for dedup)
    op.create_table(
        "listing_links",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("raw_id_a", sa.Integer(), sa.ForeignKey("raw.listings_raw.id", ondelete="CASCADE"), nullable=False),
        sa.Column("raw_id_b", sa.Integer(), sa.ForeignKey("raw.listings_raw.id", ondelete="CASCADE"), nullable=False),
        sa.Column("confidence", sa.Numeric(5, 4), nullable=False),
        sa.Column("reason", sa.String(255)),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        schema="core",
    )

    # ═══════════════════════════════════════════════════════════════════════════════
    # SCHEMA: ops — Operational Data
    # ═══════════════════════════════════════════════════════════════════════════════

    # Job Log
    op.create_table(
        "job_log",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("city_id", sa.Integer()),
        sa.Column("status", sa.Enum("pending", "running", "success", "failed", "dead", name="jobstatus", schema="ops"), server_default="pending"),
        sa.Column("retry_count", sa.Integer(), server_default="0"),
        sa.Column("error_message", sa.Text()),
        sa.Column("listings_found", sa.Integer(), server_default="0"),
        sa.Column("started_at", sa.DateTime()),
        sa.Column("completed_at", sa.DateTime()),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        schema="ops",
    )
    op.create_index("ix_ops_job_log_source_city", "job_log", ["source", "city_id"], schema="ops")
    op.create_index("ix_ops_job_log_status", "job_log", ["status"], schema="ops")

    # Event Outbox
    op.create_table(
        "event_outbox",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("workspace_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.Enum("new", "updated", name="eventtype", schema="ops"), nullable=False),
        sa.Column("listing_raw_id", sa.Integer(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), server_default="{}"),
        sa.Column("status", sa.Enum("pending", "delivering", "delivered", "failed", "dead", name="eventstatus", schema="ops"), server_default="pending"),
        sa.Column("retry_count", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("processed_at", sa.DateTime()),
        schema="ops",
    )
    op.create_index("ix_ops_event_outbox_status", "event_outbox", ["status"], schema="ops")
    op.create_index("ix_ops_event_outbox_workspace", "event_outbox", ["workspace_id"], schema="ops")

    # Delivery Log
    op.create_table(
        "delivery_log",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("workspace_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("telegram_message_id", sa.BigInteger()),
        sa.Column("sent_at", sa.DateTime(), server_default=sa.func.now()),
        schema="ops",
    )
    op.create_index("ix_ops_delivery_log_workspace_event", "delivery_log", ["workspace_id", "event_id"], unique=True, schema="ops")

    # Source Circuit Breakers
    op.create_table(
        "source_circuit_breakers",
        sa.Column("source", sa.String(50), primary_key=True),
        sa.Column("state", sa.Enum("closed", "open", "half_open", name="circuitstate", schema="ops"), server_default="closed"),
        sa.Column("failure_count", sa.Integer(), server_default="0"),
        sa.Column("last_failure_at", sa.DateTime()),
        sa.Column("recovery_at", sa.DateTime()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        schema="ops",
    )

    # Watermarks (for incremental sync)
    op.create_table(
        "watermarks",
        sa.Column("key", sa.String(100), primary_key=True),
        sa.Column("value", sa.String(100)),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        schema="ops",
    )

    # ═══════════════════════════════════════════════════════════════════════════════
    # SCHEMA: pub — Public Listings (T027.2, T027.3)
    # ═══════════════════════════════════════════════════════════════════════════════

    op.create_table(
        "public_listings",
        sa.Column("listing_id", sa.Integer(), primary_key=True),
        sa.Column("raw_id", sa.Integer(), unique=True, nullable=False),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("url", sa.String(2048), nullable=False),
        sa.Column("price", sa.Numeric(12, 2)),
        sa.Column("beds", sa.Integer()),
        sa.Column("baths", sa.Integer()),
        sa.Column("property_type", sa.String(50)),
        sa.Column("county", sa.String(100)),
        sa.Column("city", sa.String(255)),
        sa.Column("area_text", sa.String(500)),
        sa.Column("first_photo_url", sa.String(2048)),
        sa.Column("published_at", sa.DateTime()),
        sa.Column("updated_at", sa.DateTime()),
        sa.Column("status", sa.String(20), server_default="active"),
        schema="pub",
    )

    # Indexes on pub.public_listings (T027.3)
    op.create_index("ix_pub_public_listings_updated_at", "public_listings", ["updated_at"], schema="pub")
    op.create_index("ix_pub_public_listings_county", "public_listings", ["county"], schema="pub")
    op.create_index("ix_pub_public_listings_city", "public_listings", ["city"], schema="pub")
    op.create_index("ix_pub_public_listings_price", "public_listings", ["price"], schema="pub")
    op.create_index("ix_pub_public_listings_beds", "public_listings", ["beds"], schema="pub")


def downgrade() -> None:
    # Drop tables in reverse order
    op.drop_table("public_listings", schema="pub")
    op.drop_table("watermarks", schema="ops")
    op.drop_table("source_circuit_breakers", schema="ops")
    op.drop_table("delivery_log", schema="ops")
    op.drop_table("event_outbox", schema="ops")
    op.drop_table("job_log", schema="ops")
    op.drop_table("listing_links", schema="core")
    op.drop_table("listings_normalized", schema="core")
    op.drop_table("listings_raw", schema="raw")
    op.drop_table("cities", schema="core")
    op.drop_table("message_templates", schema="bot")
    op.drop_table("subscriptions", schema="bot")
    op.drop_table("workspace_admins", schema="bot")
    op.drop_table("workspaces", schema="bot")

    # Drop enums
    op.execute("DROP TYPE IF EXISTS ops.circuitstate")
    op.execute("DROP TYPE IF EXISTS ops.eventstatus")
    op.execute("DROP TYPE IF EXISTS ops.eventtype")
    op.execute("DROP TYPE IF EXISTS ops.jobstatus")
    op.execute("DROP TYPE IF EXISTS core.listingstatus")
    op.execute("DROP TYPE IF EXISTS core.propertytype")
    op.execute("DROP TYPE IF EXISTS bot.deliverymode")
    op.execute("DROP TYPE IF EXISTS bot.workspacetype")

    # Drop schemas
    op.execute("DROP SCHEMA IF EXISTS pub CASCADE")
    op.execute("DROP SCHEMA IF EXISTS ops CASCADE")
    op.execute("DROP SCHEMA IF EXISTS core CASCADE")
    op.execute("DROP SCHEMA IF EXISTS raw CASCADE")
    op.execute("DROP SCHEMA IF EXISTS bot CASCADE")
