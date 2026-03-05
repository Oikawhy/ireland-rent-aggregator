"""
AGPARS Scheduler Service

Periodically creates scrape jobs for all active subscriptions.
Run with: python -m services.scheduler
"""

import asyncio
import random

from packages.observability.logger import get_logger

logger = get_logger("service.scheduler")

# Randomized scheduling interval: 300–600 minutes (5–10 hours)
# Avoids predictable request patterns that anti-bot systems detect
# Also prevents queue backup: collector processes jobs sequentially,
# each taking 2–60 min, so full cycle takes ~2 hours.
SCHEDULE_MIN_MINUTES = 900
SCHEDULE_MAX_MINUTES = 1500


async def run_scheduler_loop():
    import os
    from packages.observability.metrics import start_metrics_server

    metrics_port = int(os.environ.get("METRICS_PORT", 8000))
    start_metrics_server(port=metrics_port)

    from packages.observability.metrics import init_scheduler_metrics
    init_scheduler_metrics()

    logger.info(
        "Starting Scheduler Service...",
        metrics_port=metrics_port,
        interval_range=f"{SCHEDULE_MIN_MINUTES}-{SCHEDULE_MAX_MINUTES}min",
    )

    first_run = True

    while True:
        try:
            # On startup: run immediately. After that: wait between cycles.
            if not first_run:
                delay_minutes = random.randint(SCHEDULE_MIN_MINUTES, SCHEDULE_MAX_MINUTES)
                delay_seconds = delay_minutes * 60
                logger.info(f"Next scheduling in {delay_minutes} minutes")

                # Expose next run time to Prometheus for Grafana dashboard
                import time
                from packages.observability.metrics import SCHEDULER_NEXT_RUN_TS
                next_run_ts = (time.time() + delay_seconds) * 1000  # ms for Grafana
                SCHEDULER_NEXT_RUN_TS.set(next_run_ts)

                await asyncio.sleep(delay_seconds)
            first_run = False

            from services.scheduler.schedule_jobs import create_scrape_jobs
            from packages.storage.queues import get_queue_stats
            from packages.observability.metrics import (
                CRAWL_QUEUE_DEPTH,
                RETRY_QUEUE_DEPTH,
            )

            logger.info("Creating scrape jobs...")
            jobs = create_scrape_jobs()
            logger.info("Scrape jobs created", count=len(jobs))

            # Update queue depth gauges
            try:
                stats = get_queue_stats()
                CRAWL_QUEUE_DEPTH.set(stats.get("pending", 0))
                RETRY_QUEUE_DEPTH.set(stats.get("retry", 0))
            except Exception as qe:
                logger.warning("Failed to update queue metrics", error=str(qe))

        except KeyboardInterrupt:
            logger.info("Scheduler stopping...")
            break
        except Exception as e:
            logger.error("Scheduler loop error", error=str(e))
            # Retry after 60–120 seconds on error (also randomized)
            await asyncio.sleep(random.randint(60, 120))


if __name__ == "__main__":
    try:
        asyncio.run(run_scheduler_loop())
    except KeyboardInterrupt:
        pass
