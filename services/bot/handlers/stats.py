"""
AGPARS Stats Handler

/stats — Pipeline statistics display.

Covers T065.11.
"""

from packages.observability.logger import get_logger
from services.bot.middleware.auth import AuthContext, require_workspace

logger = get_logger(__name__)


@require_workspace
async def handle_stats(update, context, auth_ctx: AuthContext) -> None:
    """
    /stats — Show pipeline statistics.
    """
    from services.bot.queries.listings import get_listing_count

    stats = _gather_stats()

    lines = [
        "📊 *Pipeline Statistics*\n",
        f"🏠 Active listings: *{stats['active_listings']}*",
        f"🌐 Sources configured: *{stats['sources']}*",
    ]

    if stats.get("queue_stats"):
        qs = stats["queue_stats"]
        lines.extend([
            "",
            "*Queue Status:*",
            f"  Pending: {qs.get('pending', 0)}",
            f"  Processing: {qs.get('processing', 0)}",
            f"  Retry: {qs.get('retry', 0)}",
        ])

    if stats.get("outbox_stats"):
        os = stats["outbox_stats"]
        lines.extend([
            "",
            "*Outbox Status:*",
            f"  Pending: {os.get('pending', 0)}",
            f"  Delivered: {os.get('delivered', 0)}",
            f"  Failed: {os.get('failed', 0)}",
        ])

    message = "\n".join(lines)
    await update.message.reply_text(message, parse_mode="Markdown")


def _gather_stats() -> dict:
    """Gather pipeline statistics from various sources."""
    stats = {
        "active_listings": 0,
        "sources": 6,
    }

    # Listing count
    try:
        from services.bot.queries.listings import get_listing_count
        stats["active_listings"] = get_listing_count()
    except Exception as e:
        logger.warning("Failed to get listing count", error=str(e))

    # Queue stats
    try:
        from packages.storage.queues import get_queue_stats
        stats["queue_stats"] = get_queue_stats()
    except Exception as e:
        logger.warning("Failed to get queue stats", error=str(e))

    # Outbox stats
    try:
        from sqlalchemy import text
        from packages.storage.db import get_readonly_session

        with get_readonly_session() as session:
            result = session.execute(text(
                "SELECT status, COUNT(*) as cnt FROM ops.event_outbox GROUP BY status"
            ))
            outbox = {row.status: row.cnt for row in result.fetchall()}
            stats["outbox_stats"] = outbox
    except Exception as e:
        logger.warning("Failed to get outbox stats", error=str(e))

    return stats
