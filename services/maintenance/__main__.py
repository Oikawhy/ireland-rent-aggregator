"""
AGPARS Maintenance Service

Runs daily cleanup jobs to enforce retention policies.
"""

import asyncio
from datetime import datetime, time as dt_time, timedelta, timezone

from packages.observability.logger import get_logger
from services.maintenance.retention import run_retention_cleanup

logger = get_logger("service.maintenance")


async def wait_until_target_time(target_hour: int, target_minute: int):
    """Wait until the next occurrence of the target time."""
    now = datetime.now(timezone.utc)
    target = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    
    if target <= now:
        # Target time has passed today, schedule for tomorrow
        target += timedelta(days=1)
        
    wait_seconds = (target - now).total_seconds()
    logger.info(f"Scheduling next cleanup for {target.isoformat()} (in {wait_seconds/3600:.1f} hours)")
    await asyncio.sleep(wait_seconds)


async def run_maintenance_loop():
    import os
    from packages.observability.metrics import start_metrics_server

    metrics_port = int(os.environ.get("METRICS_PORT", 8000))
    start_metrics_server(port=metrics_port)
    logger.info("Starting Maintenance Service...", metrics_port=metrics_port)
    
    # Run once on startup to ensure things are clean (optional, keeping it safe)
    logger.info("Running startup cleanup check...")
    try:
        run_retention_cleanup()
    except Exception as e:
        logger.error("Startup cleanup failed", error=str(e))

    while True:
        try:
            # Schedule for 03:00 UTC
            await wait_until_target_time(3, 0)
            
            logger.info("Starting daily cleanup...")
            run_retention_cleanup()
            logger.info("Daily cleanup finished")

        except KeyboardInterrupt:
            logger.info("Maintenance stopping...")
            break
        except Exception as e:
            logger.error("Maintenance loop error", error=str(e))
            # Sleep a bit to avoid rapid retry loops on persistent error
            await asyncio.sleep(300)


if __name__ == "__main__":
    try:
        asyncio.run(run_maintenance_loop())
    except KeyboardInterrupt:
        pass
