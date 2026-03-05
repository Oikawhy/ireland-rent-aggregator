"""
AGPARS Listing Links Storage

Storage for cross-source duplicate links.

Covers T077.
"""

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_readonly_session, get_session

logger = get_logger(__name__)


def create_listing_link(
    raw_id_a: int,
    raw_id_b: int,
    confidence: float = 0.0,
    reason: str = "cross_source",
) -> int | None:
    """
    Create a link between two duplicate listings.

    Args:
        raw_id_a: First raw listing ID
        raw_id_b: Second raw listing ID
        confidence: Similarity score (0.0-1.0)
        reason: Why they were linked (cross_source, exact)

    Returns:
        Link ID or None if link already exists
    """
    # Ensure consistent ordering (smaller ID first)
    id_a, id_b = sorted([raw_id_a, raw_id_b])

    with get_session() as session:
        # Check if link already exists
        existing = session.execute(
            text("""
                SELECT id FROM core.listing_links
                WHERE raw_id_a = :id_a AND raw_id_b = :id_b
            """),
            {"id_a": id_a, "id_b": id_b},
        ).scalar_one_or_none()

        if existing:
            return None  # Already linked

        result = session.execute(
            text("""
                INSERT INTO core.listing_links
                    (raw_id_a, raw_id_b, confidence, reason)
                VALUES (:id_a, :id_b, :confidence, :reason)
                RETURNING id
            """),
            {
                "id_a": id_a,
                "id_b": id_b,
                "confidence": confidence,
                "reason": reason,
            },
        )
        link_id = result.scalar_one()
        session.commit()

        logger.info(
            "Listing link created",
            link_id=link_id,
            raw_id_a=id_a,
            raw_id_b=id_b,
            reason=reason,
            confidence=confidence,
        )
        return link_id


def get_linked_listings(raw_id: int) -> list[dict]:
    """
    Get all listings linked to a given listing.

    Returns:
        List of linked listing dicts with link metadata
    """
    query = """
        SELECT
            CASE WHEN raw_id_a = :id THEN raw_id_b ELSE raw_id_a END AS linked_raw_id,
            confidence,
            reason,
            created_at
        FROM core.listing_links
        WHERE raw_id_a = :id OR raw_id_b = :id
        ORDER BY confidence DESC
    """

    with get_readonly_session() as session:
        result = session.execute(text(query), {"id": raw_id})
        return [dict(row._mapping) for row in result.fetchall()]


def get_duplicate_count() -> int:
    """Get total number of duplicate links."""
    with get_readonly_session() as session:
        result = session.execute(text("SELECT COUNT(*) FROM core.listing_links"))
        return result.scalar() or 0


def delete_link(link_id: int) -> bool:
    """Delete a listing link."""
    with get_session() as session:
        result = session.execute(
            text("DELETE FROM core.listing_links WHERE id = :id"),
            {"id": link_id},
        )
        session.commit()
        return result.rowcount > 0
