"""
AGPARS Dedup Service

Runs periodic deduplication jobs to find and link duplicate listings.
Flow: Core -> [Dedup] -> Listing Links
"""

import asyncio

from packages.observability.logger import get_logger
from services.dedup.scheduler import run_dedup_loop

logger = get_logger("service.dedup")


async def main():
    import os
    from packages.observability.metrics import start_metrics_server

    metrics_port = int(os.environ.get("METRICS_PORT", 8000))
    start_metrics_server(port=metrics_port)
    logger.info("Starting Dedup Service...", metrics_port=metrics_port)

    try:
        await run_dedup_loop(interval_seconds=300)
    except KeyboardInterrupt:
        logger.info("Dedup service stopping...")
    except Exception as e:
        logger.error("Dedup service crashed", error=str(e))
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
