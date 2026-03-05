"""
AGPARS Prometheus Metrics Module

Provides metrics collection and exposure for observability.
All metrics follow the taxonomy defined in ARCHITECT.md.
"""

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server

from packages.core.config import get_settings

# ═══════════════════════════════════════════════════════════════════════════════
# APPLICATION INFO
# ═══════════════════════════════════════════════════════════════════════════════

APP_INFO = Info("agpars", "Application information")

# ═══════════════════════════════════════════════════════════════════════════════
# COLLECTOR / SCRAPER METRICS
# ═══════════════════════════════════════════════════════════════════════════════

SCRAPE_JOBS_TOTAL = Counter(
    "scrape_jobs_total",
    "Total scrape jobs executed",
    ["source", "status"],
)

SCRAPE_DURATION_SECONDS = Histogram(
    "scrape_duration_seconds",
    "Time spent per scrape job",
    ["source"],
    buckets=[1, 5, 10, 30, 60, 120, 300],
)

SCRAPE_ERRORS_TOTAL = Counter(
    "scrape_errors_total",
    "Scrape failures by reason",
    ["source", "reason"],
)

LISTINGS_FOUND_TOTAL = Counter(
    "listings_found_total",
    "Raw listings discovered",
    ["source", "city"],
)

PARSE_FAILURES_TOTAL = Counter(
    "parse_failures_total",
    "Field extraction failures",
    ["source", "field"],
)

CIRCUIT_BREAKER_STATE = Gauge(
    "circuit_breaker_state",
    "Circuit breaker state (0=CLOSED, 1=HALF_OPEN, 2=OPEN)",
    ["source"],
)

# ═══════════════════════════════════════════════════════════════════════════════
# NORMALIZATION / RULES METRICS
# ═══════════════════════════════════════════════════════════════════════════════

LISTINGS_NORMALIZED_TOTAL = Counter(
    "listings_normalized_total",
    "Successfully normalized listings",
    ["source"],
)

LISTINGS_EXCLUDED_TOTAL = Counter(
    "listings_excluded_total",
    "Excluded listings",
    ["reason", "source"],
)

LEASE_LENGTH_UNKNOWN_TOTAL = Counter(
    "lease_length_unknown_total",
    "Listings with unknown lease length",
    ["source"],
)

# ═══════════════════════════════════════════════════════════════════════════════
# DEDUP / CHANGE DETECTOR METRICS
# ═══════════════════════════════════════════════════════════════════════════════

LISTING_LINKS_CREATED_TOTAL = Counter(
    "listing_links_created_total",
    "Cross-source duplicate links created",
    ["confidence_bucket"],
)

EVENTS_EMITTED_TOTAL = Counter(
    "events_emitted_total",
    "Events generated (NEW/UPDATED)",
    ["type"],
)

# ═══════════════════════════════════════════════════════════════════════════════
# OUTBOX / NOTIFIER METRICS
# ═══════════════════════════════════════════════════════════════════════════════

OUTBOX_PENDING = Gauge(
    "outbox_pending",
    "Current pending event count in outbox",
)

OUTBOX_DELIVERY_ATTEMPTS_TOTAL = Counter(
    "outbox_delivery_attempts_total",
    "Delivery attempts",
    ["result"],
)

TELEGRAM_SEND_DURATION_SECONDS = Histogram(
    "telegram_send_duration_seconds",
    "Telegram API latency",
    buckets=[0.1, 0.25, 0.5, 1, 2, 5],
)

TELEGRAM_RATE_LIMITED_TOTAL = Counter(
    "telegram_rate_limited_total",
    "HTTP 429 responses received from Telegram",
)

# ═══════════════════════════════════════════════════════════════════════════════
# PUBLISHER METRICS
# ═══════════════════════════════════════════════════════════════════════════════

PUBLISHER_LAST_SYNC_TS = Gauge(
    "publisher_last_sync_ts",
    "Timestamp of last successful sync",
)

PUBLISHER_ROWS_UPSERTED = Counter(
    "publisher_rows_upserted",
    "Count of rows synced per run",
)

PUBLISHER_LAG_SECONDS = Gauge(
    "publisher_lag_seconds",
    "Time since oldest unsynced record",
)

# ═══════════════════════════════════════════════════════════════════════════════
# REDIS / QUEUE METRICS
# ═══════════════════════════════════════════════════════════════════════════════

CRAWL_QUEUE_DEPTH = Gauge(
    "crawl_queue_depth",
    "Jobs waiting in crawl queue",
)

RETRY_QUEUE_DEPTH = Gauge(
    "retry_queue_depth",
    "Jobs in retry queue",
)

SCHEDULER_NEXT_RUN_TS = Gauge(
    "scheduler_next_run_timestamp",
    "Unix timestamp of the next scheduled scrape cycle",
)


# ═══════════════════════════════════════════════════════════════════════════════
# METRICS SERVER
# ═══════════════════════════════════════════════════════════════════════════════



# Known data sources — used to pre-initialize labeled metrics at startup
KNOWN_SOURCES = ["daft", "rent", "myhome", "property", "sherryfitz", "dng"]


def init_collector_metrics() -> None:
    """Pre-initialize collector/scraper metrics so they appear in Grafana immediately."""
    for source in KNOWN_SOURCES:
        SCRAPE_JOBS_TOTAL.labels(source=source, status="success")
        SCRAPE_JOBS_TOTAL.labels(source=source, status="failure")
        SCRAPE_DURATION_SECONDS.labels(source=source)
        SCRAPE_ERRORS_TOTAL.labels(source=source, reason="timeout")
        SCRAPE_ERRORS_TOTAL.labels(source=source, reason="parse")
        SCRAPE_ERRORS_TOTAL.labels(source=source, reason="network")
        LISTINGS_FOUND_TOTAL.labels(source=source, city="all")
        PARSE_FAILURES_TOTAL.labels(source=source, field="price")
        PARSE_FAILURES_TOTAL.labels(source=source, field="address")


def init_scheduler_metrics() -> None:
    """Pre-initialize scheduler metrics so they appear in Grafana immediately."""
    for source in KNOWN_SOURCES:
        CIRCUIT_BREAKER_STATE.labels(source=source).set(0)
    CRAWL_QUEUE_DEPTH.set(0)
    RETRY_QUEUE_DEPTH.set(0)


def init_normalizer_metrics() -> None:
    """Pre-initialize normalizer metrics so they appear in Grafana immediately."""
    for source in KNOWN_SOURCES:
        LISTINGS_NORMALIZED_TOTAL.labels(source=source)
        LISTINGS_EXCLUDED_TOTAL.labels(source=source, reason="student")
        LISTINGS_EXCLUDED_TOTAL.labels(source=source, reason="short_term")
        LISTINGS_EXCLUDED_TOTAL.labels(source=source, reason="northern_ireland")
        LEASE_LENGTH_UNKNOWN_TOTAL.labels(source=source)


def init_publisher_metrics() -> None:
    """Pre-initialize publisher metrics so they appear in Grafana immediately."""
    EVENTS_EMITTED_TOTAL.labels(type="new")
    EVENTS_EMITTED_TOTAL.labels(type="updated")


def init_notifier_metrics() -> None:
    """Pre-initialize notifier/outbox metrics so they appear in Grafana immediately."""
    OUTBOX_PENDING.set(0)
    OUTBOX_DELIVERY_ATTEMPTS_TOTAL.labels(result="success")
    OUTBOX_DELIVERY_ATTEMPTS_TOTAL.labels(result="failure")


def start_metrics_server(port: int | None = None) -> None:
    """
    Start the Prometheus metrics HTTP server.

    Args:
        port: Port to listen on (defaults to config value)
    """
    settings = get_settings()
    metrics_port = port or settings.observability.metrics_port

    # Set application info
    APP_INFO.info({
        "app": settings.app_name,
        "environment": settings.environment,
    })

    start_http_server(metrics_port)


def record_scrape_job(source: str, status: str, duration_seconds: float) -> None:
    """Record scrape job completion metrics."""
    SCRAPE_JOBS_TOTAL.labels(source=source, status=status).inc()
    SCRAPE_DURATION_SECONDS.labels(source=source).observe(duration_seconds)


def record_listings_found(source: str, city: str, count: int) -> None:
    """Record listings found during scrape."""
    LISTINGS_FOUND_TOTAL.labels(source=source, city=city).inc(count)


def record_exclusion(source: str, reason: str) -> None:
    """Record listing exclusion."""
    LISTINGS_EXCLUDED_TOTAL.labels(source=source, reason=reason).inc()


def record_event(event_type: str) -> None:
    """Record event emission."""
    EVENTS_EMITTED_TOTAL.labels(type=event_type).inc()


def set_circuit_breaker_state(source: str, state: int) -> None:
    """Set circuit breaker state gauge (0=CLOSED, 1=HALF_OPEN, 2=OPEN)."""
    CIRCUIT_BREAKER_STATE.labels(source=source).set(state)


# ═══════════════════════════════════════════════════════════════════════════════
# METRICS FACADE
# ═══════════════════════════════════════════════════════════════════════════════

# Registry of known counters
KNOWN_COUNTERS: dict[str, Counter] = {
    "listings_excluded_total": LISTINGS_EXCLUDED_TOTAL,
    "scrape_errors_total": SCRAPE_ERRORS_TOTAL,
    "listings_found_total": LISTINGS_FOUND_TOTAL,
    "parse_failures_total": PARSE_FAILURES_TOTAL,
    "listings_normalized_total": LISTINGS_NORMALIZED_TOTAL,
    "listing_links_created_total": LISTING_LINKS_CREATED_TOTAL,
    "events_emitted_total": EVENTS_EMITTED_TOTAL,
    "outbox_delivery_attempts_total": OUTBOX_DELIVERY_ATTEMPTS_TOTAL,
    "telegram_rate_limited_total": TELEGRAM_RATE_LIMITED_TOTAL,
    "scrape_jobs_total": SCRAPE_JOBS_TOTAL,
    "publisher_rows_upserted": PUBLISHER_ROWS_UPSERTED,
}

# Registry of known gauges
KNOWN_GAUGES: dict[str, Gauge] = {
    "circuit_breaker_state": CIRCUIT_BREAKER_STATE,
    "outbox_pending": OUTBOX_PENDING,
    "publisher_lag_seconds": PUBLISHER_LAG_SECONDS,
    "publisher_last_sync_ts": PUBLISHER_LAST_SYNC_TS,
    "crawl_queue_depth": CRAWL_QUEUE_DEPTH,
    "retry_queue_depth": RETRY_QUEUE_DEPTH,
    "scheduler_next_run_timestamp": SCHEDULER_NEXT_RUN_TS,
}


class MetricsFacade:
    """Convenience wrapper for metrics operations."""

    def gauge(self, name: str, value: float, labels: dict | None = None) -> None:
        """Set gauge value."""
        if name not in KNOWN_GAUGES:
            raise KeyError(f"Unknown gauge metric: {name}")
        metric = KNOWN_GAUGES[name]
        if labels:
            metric.labels(**labels).set(value)
        else:
            metric.set(value)

    def counter(self, name: str, value: int = 1, labels: dict | None = None) -> None:
        """Increment counter."""
        if name not in KNOWN_COUNTERS:
            raise KeyError(f"Unknown counter metric: {name}")
        metric = KNOWN_COUNTERS[name]
        if labels:
            metric.labels(**labels).inc(value)
        else:
            metric.inc(value)

    def increment(self, name: str, value: int = 1, labels: dict | None = None) -> None:
        """Alias for counter() - increments a counter metric."""
        self.counter(name, value, labels)


_metrics_instance: MetricsFacade | None = None


def get_metrics() -> MetricsFacade:
    """Get global metrics facade instance."""
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = MetricsFacade()
    return _metrics_instance
