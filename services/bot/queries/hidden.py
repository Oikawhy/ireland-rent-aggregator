"""
AGPARS Hidden Listings Queries

CRUD for bot.hidden_listings — allows users to hide/unhide listings.
"""

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_session, get_readonly_session

logger = get_logger(__name__)


def hide_listing(workspace_id: int, listing_id: int, hidden_by: int) -> bool:
    """
    Hide a listing for a workspace.

    Returns:
        True if newly hidden, False if already hidden
    """
    try:
        with get_session() as session:
            session.execute(
                text("""
                    INSERT INTO bot.hidden_listings (workspace_id, listing_id, hidden_by)
                    VALUES (:workspace_id, :listing_id, :hidden_by)
                    ON CONFLICT (workspace_id, listing_id) DO NOTHING
                """),
                {
                    "workspace_id": workspace_id,
                    "listing_id": listing_id,
                    "hidden_by": hidden_by,
                },
            )
            session.commit()
        logger.info("Listing hidden", workspace_id=workspace_id, listing_id=listing_id)
        return True
    except Exception as e:
        logger.error("Failed to hide listing", error=str(e))
        return False


def unhide_listing(workspace_id: int, listing_id: int) -> bool:
    """Unhide a listing for a workspace."""
    try:
        with get_session() as session:
            result = session.execute(
                text("""
                    DELETE FROM bot.hidden_listings
                    WHERE workspace_id = :workspace_id AND listing_id = :listing_id
                """),
                {"workspace_id": workspace_id, "listing_id": listing_id},
            )
            session.commit()
        return result.rowcount > 0
    except Exception as e:
        logger.error("Failed to unhide listing", error=str(e))
        return False


def get_hidden_listing_ids(workspace_id: int) -> list[int]:
    """Get all hidden listing IDs for a workspace, expanded to include cluster members.

    When a user hides listing_id=42 from daft.ie, this also returns
    listing_id=78 from dng.ie if they share the same cluster_id.
    This ensures the entire duplicate cluster is excluded from results.
    """
    with get_readonly_session() as session:
        result = session.execute(
            text("""
                WITH directly_hidden AS (
                    SELECT listing_id
                    FROM bot.hidden_listings
                    WHERE workspace_id = :workspace_id
                ),
                hidden_clusters AS (
                    SELECT DISTINCT cluster_id
                    FROM pub.public_listings
                    WHERE listing_id IN (SELECT listing_id FROM directly_hidden)
                      AND cluster_id IS NOT NULL
                )
                -- Directly hidden listings (including those without a cluster)
                SELECT listing_id FROM directly_hidden
                UNION
                -- All cluster members of hidden listings
                SELECT pl.listing_id
                FROM pub.public_listings pl
                WHERE pl.cluster_id IN (SELECT cluster_id FROM hidden_clusters)
            """),
            {"workspace_id": workspace_id},
        )
        return [row.listing_id for row in result.fetchall()]


def get_hidden_listings(workspace_id: int, limit: int = 50, offset: int = 0) -> list[dict]:
    """Get hidden listings with details from pub.public_listings."""
    with get_readonly_session() as session:
        result = session.execute(
            text("""
                SELECT pl.listing_id, pl.source, pl.url, pl.price, pl.beds,
                       pl.city, pl.county, pl.property_type,
                       hl.hidden_at
                FROM bot.hidden_listings hl
                JOIN pub.public_listings pl ON pl.listing_id = hl.listing_id
                WHERE hl.workspace_id = :workspace_id
                ORDER BY hl.hidden_at DESC
                LIMIT :limit OFFSET :offset
            """),
            {"workspace_id": workspace_id, "limit": limit, "offset": offset},
        )
        return [dict(row._mapping) for row in result.fetchall()]


def get_hidden_count(workspace_id: int) -> int:
    """Get count of hidden listings for a workspace."""
    with get_readonly_session() as session:
        result = session.execute(
            text("""
                SELECT COUNT(*) FROM bot.hidden_listings
                WHERE workspace_id = :workspace_id
            """),
            {"workspace_id": workspace_id},
        )
        return result.scalar() or 0


def is_hidden(workspace_id: int, listing_id: int) -> bool:
    """Check if a listing is hidden for a workspace."""
    with get_readonly_session() as session:
        result = session.execute(
            text("""
                SELECT 1 FROM bot.hidden_listings
                WHERE workspace_id = :workspace_id AND listing_id = :listing_id
            """),
            {"workspace_id": workspace_id, "listing_id": listing_id},
        )
        return result.fetchone() is not None
