"""
AGPARS Normalizer Service

Continuously monitors raw listings and normalizes them into core listings.
Flow: Raw -> [Normalizer] -> Core
"""

import asyncio
import time
from sqlalchemy import text as sa_text

from packages.storage.db import get_session
from packages.observability.logger import get_logger
from packages.observability.metrics import LISTINGS_NORMALIZED_TOTAL
from services.normalizer.normalize import process_raw_listing

logger = get_logger("service.normalizer")


def get_unnormalized_ids(limit: int = 100) -> list[tuple[int, str]]:
    """Fetch raw IDs (with source) that don't have a normalized counterpart."""
    query = """
        SELECT r.id, r.source
        FROM raw.listings_raw r
        LEFT JOIN core.listings_normalized n ON r.id = n.raw_id
        WHERE n.id IS NULL
        ORDER BY r.id ASC
        LIMIT :limit
    """
    with get_session() as session:
        result = session.execute(sa_text(query), {"limit": limit})
        return [(row[0], row[1]) for row in result.fetchall()]


async def run_normalizer_loop():
    import os
    from packages.observability.metrics import start_metrics_server

    metrics_port = int(os.environ.get("METRICS_PORT", 8000))
    start_metrics_server(port=metrics_port)

    from packages.observability.metrics import init_normalizer_metrics
    init_normalizer_metrics()

    logger.info("Starting Normalizer Service...", metrics_port=metrics_port)
    
    while True:
        try:
            start_time = time.time()
            raw_entries = get_unnormalized_ids()
            
            if not raw_entries:
                # No work, sleep and continue
                await asyncio.sleep(5)
                continue

            success = 0
            for raw_id, source in raw_entries:
                try:
                    result = process_raw_listing(raw_id)
                    if result:
                        success += 1
                        LISTINGS_NORMALIZED_TOTAL.labels(source=source).inc()
                except Exception as e:
                    logger.error("Normalization failed", raw_id=raw_id, error=str(e))
            
            duration = time.time() - start_time
            logger.info(
                "Batch normalized",
                count=success,
                total=len(raw_entries),
                duration=f"{duration:.2f}s"
            )
            
            # Small sleep to prevent tight loop if processing is instant
            await asyncio.sleep(1)

        except KeyboardInterrupt:
            logger.info("Normalizer stopping...")
            break
        except Exception as e:
            logger.error("Normalizer loop error", error=str(e))
            await asyncio.sleep(10)


if __name__ == "__main__":
    try:
        asyncio.run(run_normalizer_loop())
    except KeyboardInterrupt:
        pass
