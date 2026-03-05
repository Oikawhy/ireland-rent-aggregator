"""
AGPARS Collector Service

Worker process that dequeues scrape jobs from Redis and runs them
using the CollectorRunner + source-specific adapters.
Run with: python -m services.collector
"""

import asyncio

import redis

from packages.observability.logger import get_logger
from packages.storage.redis import reset_redis_client

logger = get_logger("service.collector")


async def run_collector_loop():
    import os
    from packages.observability.metrics import start_metrics_server

    metrics_port = int(os.environ.get("METRICS_PORT", 8000))
    start_metrics_server(port=metrics_port)

    from packages.observability.metrics import init_collector_metrics
    init_collector_metrics()

    logger.info("Starting Collector Service...", metrics_port=metrics_port)

    from services.collector.runner import CollectorRunner, ScrapeJob
    from services.collector.adapters import ADAPTERS
    from packages.storage.queues import dequeue_job

    # Instantiate all registered source adapters
    adapters = {name: cls() for name, cls in ADAPTERS.items()}
    runner = CollectorRunner(adapters=adapters)

    try:
        await runner.start()
        logger.info("Collector browser started")

        while True:
            try:
                # dequeue_job() uses blpop with 5s timeout, so it blocks
                job_data = dequeue_job()

                if not job_data:
                    continue

                job = ScrapeJob(
                    source=job_data.source,
                    city=job_data.city_name,
                    county=job_data.county,
                    city_id=job_data.city_id,
                )

                logger.info(
                    "Processing job",
                    source=job.source,
                    city=job.city,
                    county=job.county,
                )

                result = await runner.run_job(job)

                logger.info(
                    "Job completed",
                    source=result.source,
                    listings=len(result.listings),
                    success=result.success,
                )

            except KeyboardInterrupt:
                break
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.warning("Redis error, resetting client", error=str(e))
                reset_redis_client()
                await asyncio.sleep(30)
            except (ConnectionError, OSError, TimeoutError) as e:
                logger.warning("Network error, backing off", error=str(e))
                await asyncio.sleep(30)
            except Exception as e:
                logger.error("Collector job error", error=str(e))
                await asyncio.sleep(10)

    finally:
        await runner.stop()
        logger.info("Collector stopped")


if __name__ == "__main__":
    try:
        asyncio.run(run_collector_loop())
    except KeyboardInterrupt:
        pass

