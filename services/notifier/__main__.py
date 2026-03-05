"""
AGPARS Notifier — Entry Point

Runs the OutboxWorker (instant delivery), DigestScheduler (periodic digests),
and RetryManager (failed event recovery).

Run with: python -m services.notifier
"""

import asyncio
import signal

from packages.observability.logger import get_logger
from services.notifier.digest_scheduler import DigestScheduler
from services.notifier.outbox_worker import OutboxWorker
from services.publisher.event_outbox import retry_failed_events

logger = get_logger(__name__)


async def run_retry_loop():
    """Periodically retry failed events."""
    logger.info("Retry loop started")
    while True:
        try:
            count = retry_failed_events()
            if count > 0:
                logger.info("Retried failed events", count=count)
            # Check every minute
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Retry loop error", error=str(e))
            await asyncio.sleep(60)


async def main() -> None:
    """Start the Notifier service."""
    import os
    from packages.observability.metrics import start_metrics_server

    metrics_port = int(os.environ.get("METRICS_PORT", 8000))
    start_metrics_server(port=metrics_port)

    from packages.observability.metrics import init_notifier_metrics
    init_notifier_metrics()

    logger.info("Starting AGPARS Notifier...", metrics_port=metrics_port)

    # Initialize workers
    outbox_worker = OutboxWorker()
    digest_scheduler = DigestScheduler()

    # Create tasks
    worker_task = asyncio.create_task(outbox_worker.run())
    digest_task = asyncio.create_task(digest_scheduler.run())
    retry_task = asyncio.create_task(run_retry_loop())

    # Graceful shutdown handler
    stop_event = asyncio.Event()

    def shutdown(sig, frame):
        logger.info("Shutdown signal received", signal=sig)
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Wait for stop signal
    await stop_event.wait()

    # Stop workers
    logger.info("Stopping services...")
    outbox_worker.stop()
    digest_scheduler.stop()
    retry_task.cancel()

    # Wait for completion (with timeout)
    try:
        await asyncio.wait_for(
            asyncio.gather(worker_task, digest_task, retry_task, return_exceptions=True),
            timeout=5.0
        )
    except asyncio.TimeoutError:
        logger.warning("Forced shutdown after timeout")

    logger.info("Notifier stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
