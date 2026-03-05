# ═══════════════════════════════════════════════════════════════════════════════
# AGPARS – Unified Dockerfile (multi-stage)
# All services share this image, launched with different commands via compose.
# ═══════════════════════════════════════════════════════════════════════════════

# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System deps needed to compile psycopg2, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Non-root user for security
RUN groupadd -r agpars && useradd -r -g agpars -d /app -s /sbin/nologin agpars

WORKDIR /app

# Runtime system deps (libpq5 for postgres, curl for healthchecks,
# chromium + chromium-driver for undetected-chromedriver / Daft.ie,
# xvfb for virtual display — needed by UC Chrome to bypass Cloudflare)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 curl chromium chromium-driver xvfb && \
    rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY packages/ ./packages/
COPY services/ ./services/
COPY scripts/ ./scripts/
COPY alembic.ini ./

# Install Playwright browsers into the app dir so the non-root user can find them
ENV PLAYWRIGHT_BROWSERS_PATH=/app/.cache/ms-playwright
RUN python -m playwright install --with-deps chromium

# Copy chromedriver to writable location — undetected-chromedriver patches
# the binary in-place (removes $cdc_ markers), which requires write access.
# /usr/bin/ is owned by root, so the agpars user can't modify it there.
RUN cp /usr/bin/chromedriver /app/chromedriver && chmod +x /app/chromedriver
RUN cp /usr/bin/chromium /app/chromium && chmod +x /app/chromium

# Ensure the app dir (incl. browser cache) is owned by agpars
RUN chown -R agpars:agpars /app

USER agpars

# Default: no command – each service specifies its own in docker-compose
CMD ["python", "--version"]
