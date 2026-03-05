"""
AGPARS Publisher Service

Continuously syncs core listings to public views and generates notification events.
Flow: Core -> [Publisher] -> Pub + Event Outbox
"""

import asyncio
import time

from packages.observability.logger import get_logger
from services.publisher.sync import run_publisher_sync

logger = get_logger("service.publisher")


async def run_publisher_loop():
    import os
    from packages.observability.metrics import start_metrics_server

    metrics_port = int(os.environ.get("METRICS_PORT", 8000))
    start_metrics_server(port=metrics_port)

    from packages.observability.metrics import init_publisher_metrics
    init_publisher_metrics()

    logger.info("Starting Publisher Service...", metrics_port=metrics_port)
    
    # Run slightly more often than 60s to ensure we catch things
    interval = 10
    
    while True:
        try:
            start_time = time.time()
            
            # Run the sync logic
            # This function handles:
            # 1. Sync Core -> Pub
            # 2. Change Detection -> Events
            # 3. Metrics
            stats = run_publisher_sync()
            
            if stats.get("total_processed", 0) > 0:
                logger.info("Publisher sync stats", **stats)
            
            # Calculate sleep time to maintain interval
            elapsed = time.time() - start_time
            sleep_time = max(1.0, interval - elapsed)
            
            await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Publisher stopping...")
            break
        except Exception as e:
            logger.error("Publisher loop error", error=str(e))
            await asyncio.sleep(10)


if __name__ == "__main__":
    try:
        asyncio.run(run_publisher_loop())
    except KeyboardInterrupt:
        pass
