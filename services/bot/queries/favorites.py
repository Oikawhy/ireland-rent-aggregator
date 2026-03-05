"""
AGPARS Favorites Queries

CRUD for bot.favorites — allows users to mark/unmark favorite listings.
"""

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_session, get_readonly_session

logger = get_logger(__name__)


def toggle_favorite(workspace_id: int, listing_id: int, user_id: int) -> bool:
    """
    Toggle favorite status.

    Returns:
        True if now favorited, False if unfavorited
    """
    with get_session() as session:
        # Check if already favorited
        result = session.execute(
            text("""
                SELECT 1 FROM bot.favorites
                WHERE workspace_id = :workspace_id AND listing_id = :listing_id
            """),
            {"workspace_id": workspace_id, "listing_id": listing_id},
        )
        if result.fetchone():
            # Remove
            session.execute(
                text("""
                    DELETE FROM bot.favorites
                    WHERE workspace_id = :workspace_id AND listing_id = :listing_id
                """),
                {"workspace_id": workspace_id, "listing_id": listing_id},
            )
            session.commit()
            logger.info("Favorite removed", workspace_id=workspace_id, listing_id=listing_id)
            return False
        else:
            # Add
            session.execute(
                text("""
                    INSERT INTO bot.favorites (workspace_id, listing_id, added_by)
                    VALUES (:workspace_id, :listing_id, :added_by)
                    ON CONFLICT (workspace_id, listing_id) DO NOTHING
                """),
                {"workspace_id": workspace_id, "listing_id": listing_id, "added_by": user_id},
            )
            session.commit()
            logger.info("Favorite added", workspace_id=workspace_id, listing_id=listing_id)
            return True


def is_favorite(workspace_id: int, listing_id: int) -> bool:
    """Check if listing is favorited."""
    with get_readonly_session() as session:
        result = session.execute(
            text("""
                SELECT 1 FROM bot.favorites
                WHERE workspace_id = :workspace_id AND listing_id = :listing_id
            """),
            {"workspace_id": workspace_id, "listing_id": listing_id},
        )
        return result.fetchone() is not None


def get_favorite_ids(workspace_id: int) -> list[int]:
    """Get all favorite listing IDs."""
    with get_readonly_session() as session:
        result = session.execute(
            text("""
                SELECT listing_id FROM bot.favorites
                WHERE workspace_id = :workspace_id
                ORDER BY added_at DESC
            """),
            {"workspace_id": workspace_id},
        )
        return [row.listing_id for row in result.fetchall()]


def get_favorites(workspace_id: int, limit: int = 50, offset: int = 0) -> list[dict]:
    """Get favorites with listing details."""
    with get_readonly_session() as session:
        result = session.execute(
            text("""
                SELECT pl.listing_id, pl.source, pl.url, pl.price, pl.beds,
                       pl.baths, pl.city, pl.county, pl.property_type,
                       f.added_at
                FROM bot.favorites f
                JOIN pub.public_listings pl ON pl.listing_id = f.listing_id
                WHERE f.workspace_id = :workspace_id
                ORDER BY f.added_at DESC
                LIMIT :limit OFFSET :offset
            """),
            {"workspace_id": workspace_id, "limit": limit, "offset": offset},
        )
        return [dict(row._mapping) for row in result.fetchall()]


def get_favorites_count(workspace_id: int) -> int:
    """Get count of favorites."""
    with get_readonly_session() as session:
        result = session.execute(
            text("""
                SELECT COUNT(*) FROM bot.favorites
                WHERE workspace_id = :workspace_id
            """),
            {"workspace_id": workspace_id},
        )
        return result.scalar() or 0
