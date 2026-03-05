-- =============================================================================
-- AGPARS — Initial Database Schema (all 5 schemas)
--
-- Idempotent: safe to run multiple times.
-- Usage:  psql $DATABASE_URL -f scripts/db/init_schema.sql
-- =============================================================================

-- ─── Extensions ──────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ─── Schemas ─────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS pub;
CREATE SCHEMA IF NOT EXISTS bot;

-- ═════════════════════════════════════════════════════════════════════════════
-- SCHEMA: bot
-- ═════════════════════════════════════════════════════════════════════════════

DO $$ BEGIN
    CREATE TYPE bot.workspacetype AS ENUM ('personal', 'group');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE bot.deliverymode AS ENUM ('instant', 'digest', 'paused');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE bot.userrole AS ENUM ('unauthorized', 'regular', 'admin');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE bot.accessrequeststatus AS ENUM ('pending', 'approved', 'declined');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS bot.workspaces (
    id              SERIAL PRIMARY KEY,
    type            bot.workspacetype NOT NULL,
    tg_chat_id      BIGINT UNIQUE NOT NULL,
    title           VARCHAR(255),
    timezone        VARCHAR(50) DEFAULT 'Europe/Dublin',
    settings        JSONB DEFAULT '{}',
    is_active       BOOLEAN DEFAULT TRUE,
    owner_user_id   INTEGER,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_bot_workspaces_tg_chat_id
    ON bot.workspaces (tg_chat_id);

CREATE TABLE IF NOT EXISTS bot.users (
    id              SERIAL PRIMARY KEY,
    tg_user_id      BIGINT UNIQUE NOT NULL,
    tg_username     VARCHAR(255),
    full_name       VARCHAR(255),
    role            bot.userrole NOT NULL DEFAULT 'unauthorized',
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_bot_users_tg_user_id
    ON bot.users (tg_user_id);

-- Add FK for owner_user_id after users table exists
ALTER TABLE bot.workspaces
    ADD CONSTRAINT fk_workspaces_owner FOREIGN KEY (owner_user_id) REFERENCES bot.users(id) ON DELETE CASCADE;

CREATE TABLE IF NOT EXISTS bot.access_requests (
    id                      SERIAL PRIMARY KEY,
    user_id                 INTEGER NOT NULL REFERENCES bot.users(id) ON DELETE CASCADE,
    status                  bot.accessrequeststatus DEFAULT 'pending',
    admin_message_id        BIGINT,
    reviewed_by_user_id     INTEGER REFERENCES bot.users(id),
    created_at              TIMESTAMP DEFAULT NOW(),
    reviewed_at             TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bot.subscriptions (
    id              SERIAL PRIMARY KEY,
    workspace_id    INTEGER NOT NULL REFERENCES bot.workspaces(id) ON DELETE CASCADE,
    name            VARCHAR(100),
    filters         JSONB DEFAULT '{}',
    delivery_mode   bot.deliverymode DEFAULT 'instant',
    digest_schedule JSONB,
    is_enabled      BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot.message_templates (
    id              SERIAL PRIMARY KEY,
    workspace_id    INTEGER UNIQUE REFERENCES bot.workspaces(id) ON DELETE CASCADE,
    template        TEXT NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot.hidden_listings (
    id              SERIAL PRIMARY KEY,
    workspace_id    INTEGER NOT NULL REFERENCES bot.workspaces(id) ON DELETE CASCADE,
    listing_id      INTEGER NOT NULL,
    hidden_by       BIGINT NOT NULL,
    hidden_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(workspace_id, listing_id)
);
CREATE INDEX IF NOT EXISTS idx_hidden_listings_ws
    ON bot.hidden_listings (workspace_id);

CREATE TABLE IF NOT EXISTS bot.favorites (
    id              SERIAL PRIMARY KEY,
    workspace_id    INTEGER NOT NULL REFERENCES bot.workspaces(id) ON DELETE CASCADE,
    listing_id      INTEGER NOT NULL,
    added_by        BIGINT NOT NULL,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(workspace_id, listing_id)
);
CREATE INDEX IF NOT EXISTS idx_favorites_ws
    ON bot.favorites (workspace_id);

-- ═════════════════════════════════════════════════════════════════════════════
-- SCHEMA: core
-- ═════════════════════════════════════════════════════════════════════════════

DO $$ BEGIN
    CREATE TYPE core.propertytype AS ENUM ('apartment', 'house', 'studio', 'other');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE core.listingstatus AS ENUM ('active', 'removed');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS core.cities (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,
    county          VARCHAR(100) NOT NULL,
    population      INTEGER,
    synonyms        TEXT[]
);
CREATE INDEX IF NOT EXISTS ix_core_cities_county
    ON core.cities (county);

-- ═════════════════════════════════════════════════════════════════════════════
-- SCHEMA: raw
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS raw.listings_raw (
    id                  SERIAL PRIMARY KEY,
    source              VARCHAR(50) NOT NULL,
    source_listing_id   VARCHAR(100) NOT NULL,
    url                 VARCHAR(2048) NOT NULL,
    raw_payload         JSONB,
    first_photo_url     VARCHAR(2048),
    first_seen          TIMESTAMP DEFAULT NOW(),
    last_seen           TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_raw_listings_source
    ON raw.listings_raw (source);
CREATE UNIQUE INDEX IF NOT EXISTS ix_raw_listings_source_listing
    ON raw.listings_raw (source, source_listing_id);

-- ═════════════════════════════════════════════════════════════════════════════
-- SCHEMA: core (continued — depends on raw)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS core.listings_normalized (
    id                      SERIAL PRIMARY KEY,
    raw_id                  INTEGER UNIQUE REFERENCES raw.listings_raw(id) ON DELETE CASCADE,
    price                   NUMERIC(12,2),
    beds                    INTEGER,
    baths                   INTEGER,
    property_type           core.propertytype,
    furnished               BOOLEAN,
    lease_length_months     INTEGER,
    lease_length_unknown    BOOLEAN DEFAULT FALSE,
    city_id                 INTEGER REFERENCES core.cities(id),
    county                  VARCHAR(100),
    area_text               VARCHAR(500),
    status                  core.listingstatus DEFAULT 'active',
    search_vector           TSVECTOR,
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_core_listings_norm_city_id
    ON core.listings_normalized (city_id);
CREATE INDEX IF NOT EXISTS ix_core_listings_norm_updated_at
    ON core.listings_normalized (updated_at);

CREATE TABLE IF NOT EXISTS core.listing_links (
    id              SERIAL PRIMARY KEY,
    raw_id_a        INTEGER NOT NULL REFERENCES raw.listings_raw(id) ON DELETE CASCADE,
    raw_id_b        INTEGER NOT NULL REFERENCES raw.listings_raw(id) ON DELETE CASCADE,
    confidence      NUMERIC(5,4) NOT NULL,
    reason          VARCHAR(255),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ═════════════════════════════════════════════════════════════════════════════
-- SCHEMA: ops
-- ═════════════════════════════════════════════════════════════════════════════

DO $$ BEGIN
    CREATE TYPE ops.jobstatus AS ENUM ('pending', 'running', 'success', 'failed', 'dead');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE ops.eventtype AS ENUM ('new', 'updated');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE ops.eventstatus AS ENUM ('pending', 'delivering', 'delivered', 'failed', 'dead');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE ops.circuitstate AS ENUM ('closed', 'open', 'half_open');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS ops.job_log (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50) NOT NULL,
    city_id         INTEGER,
    status          ops.jobstatus DEFAULT 'pending',
    retry_count     INTEGER DEFAULT 0,
    error_message   TEXT,
    listings_found  INTEGER DEFAULT 0,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_ops_job_log_source_city
    ON ops.job_log (source, city_id);
CREATE INDEX IF NOT EXISTS ix_ops_job_log_status
    ON ops.job_log (status);

CREATE TABLE IF NOT EXISTS ops.event_outbox (
    id              SERIAL PRIMARY KEY,
    workspace_id    INTEGER NOT NULL,
    event_type      ops.eventtype NOT NULL,
    listing_raw_id  INTEGER NOT NULL,
    payload         JSONB DEFAULT '{}',
    status          ops.eventstatus DEFAULT 'pending',
    retry_count     INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    processed_at    TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_ops_event_outbox_status
    ON ops.event_outbox (status);
CREATE INDEX IF NOT EXISTS ix_ops_event_outbox_workspace
    ON ops.event_outbox (workspace_id);

CREATE TABLE IF NOT EXISTS ops.delivery_log (
    id                  SERIAL PRIMARY KEY,
    workspace_id        INTEGER NOT NULL,
    event_id            INTEGER NOT NULL,
    telegram_message_id BIGINT,
    sent_at             TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_ops_delivery_log_workspace_event
    ON ops.delivery_log (workspace_id, event_id);

CREATE TABLE IF NOT EXISTS ops.source_circuit_breakers (
    source          VARCHAR(50) PRIMARY KEY,
    state           ops.circuitstate DEFAULT 'closed',
    failure_count   INTEGER DEFAULT 0,
    last_failure_at TIMESTAMP,
    recovery_at     TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.watermarks (
    key             VARCHAR(100) PRIMARY KEY,
    value           VARCHAR(100),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ═════════════════════════════════════════════════════════════════════════════
-- SCHEMA: pub
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pub.public_listings (
    listing_id      SERIAL PRIMARY KEY,
    raw_id          INTEGER UNIQUE NOT NULL,
    source          VARCHAR(50) NOT NULL,
    url             VARCHAR(2048) NOT NULL,
    price           NUMERIC(12,2),
    beds            INTEGER,
    baths           INTEGER,
    property_type   VARCHAR(50),
    county          VARCHAR(100),
    city            VARCHAR(255),
    area_text       VARCHAR(500),
    first_photo_url VARCHAR(2048),
    published_at    TIMESTAMP,
    updated_at      TIMESTAMP,
    status          VARCHAR(20) DEFAULT 'active'
);
CREATE INDEX IF NOT EXISTS ix_pub_public_listings_updated_at
    ON pub.public_listings (updated_at);
CREATE INDEX IF NOT EXISTS ix_pub_public_listings_county
    ON pub.public_listings (county);
CREATE INDEX IF NOT EXISTS ix_pub_public_listings_city
    ON pub.public_listings (city);
CREATE INDEX IF NOT EXISTS ix_pub_public_listings_price
    ON pub.public_listings (price);
CREATE INDEX IF NOT EXISTS ix_pub_public_listings_beds
    ON pub.public_listings (beds);

-- ═════════════════════════════════════════════════════════════════════════════
-- Alembic version table (mark migration as applied)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS public.alembic_version (
    version_num VARCHAR(32) NOT NULL,
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Mark the initial migration as already applied (idempotent)
INSERT INTO public.alembic_version (version_num)
VALUES ('001_initial_schema')
ON CONFLICT (version_num) DO NOTHING;

-- ═════════════════════════════════════════════════════════════════════════════
-- Done!
-- ═════════════════════════════════════════════════════════════════════════════
