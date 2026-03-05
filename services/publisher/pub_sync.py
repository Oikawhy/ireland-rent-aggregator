"""
AGPARS Publisher Pub Schema Sync

Maintain pub.public_listings from core.listings_normalized.
"""

import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text

from packages.observability.logger import get_logger
from packages.storage.db import get_session_context

logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CITY PARSING FROM area_text
# ═══════════════════════════════════════════════════════════════════════════════

# Patterns to detect county segments ("Co. Dublin", "County Cork", "Dublin" alone)
_COUNTY_RE = re.compile(
    r"^\s*(?:co\.?|county)\s+",
    re.IGNORECASE,
)

_IRISH_COUNTIES = {
    c.lower()
    for c in [
        "Carlow", "Cavan", "Clare", "Cork", "Donegal", "Dublin",
        "Galway", "Kerry", "Kildare", "Kilkenny", "Laois", "Leitrim",
        "Limerick", "Longford", "Louth", "Mayo", "Meath", "Monaghan",
        "Offaly", "Roscommon", "Sligo", "Tipperary", "Waterford",
        "Westmeath", "Wexford", "Wicklow",
    ]
}

# Dublin postal codes: "Dublin 6", "Dublin 6W", "D14"
_DUBLIN_POSTAL_RE = re.compile(
    r"^\s*(?:dublin\s*\d{1,2}w?|d\d{1,2}w?)\s*$",
    re.IGNORECASE,
)

# Segment starts with digits → likely house number / apt
_HOUSE_NUM_RE = re.compile(r"^\s*\d")

# Irish Eircode: routing key (letter + 2 digits) + optional 4 alphanumeric
# e.g. "D15 FH01", "X91DWX8", "A63 DY24", "D02"
_EIRCODE_RE = re.compile(
    r"^\s*[A-Za-z]\d{2}\s?[A-Za-z0-9]{4}\s*$"  # Full Eircode
    r"|^\s*[A-Za-z]\d{2}\s*$",                    # Routing key only
)

# Property reference codes from estate agents (e.g. "RN715F", "KK123", "WD4521")
# Pattern: short alphanumeric string (3-8 chars) with mixed letters AND digits,
# not a real place name.
_PROP_REF_RE = re.compile(
    r"^\s*(?=[A-Za-z0-9]*\d)(?=[A-Za-z0-9]*[A-Za-z])[A-Za-z0-9]{3,8}\s*$"
)


def parse_city_from_area(area_text: str | None, county: str | None) -> str | None:
    """
    Extract city/area name from Irish address string.

    Examples:
        "5 Maypark Mews, Dunmore Road, Ardkeen, Co. Waterford"  → "Ardkeen"
        "Apt 3, Rathmines, Dublin 6"                            → "Rathmines"
        "Virginia, Co. Cavan"                                   → "Virginia"
        "Cork City Centre"                                      → "Cork City Centre"
        "Dublin 8"                                              → "Dublin 8"
    """
    if not area_text:
        return None

    parts = [p.strip() for p in area_text.split(",") if p.strip()]
    if not parts:
        return None

    county_lower = (county or "").lower()

    # Filter out county segments and house-number segments
    candidates = []
    for part in parts:
        p_lower = part.lower().strip()

        # Skip "Co. Waterford", "County Cork"
        if _COUNTY_RE.match(p_lower):
            continue

        # Skip bare county name if it matches exactly ("Cork", "Dublin")
        # BUT keep Dublin postal codes ("Dublin 8") and compound names ("Cork City Centre")
        if p_lower in _IRISH_COUNTIES and not _DUBLIN_POSTAL_RE.match(p_lower):
            # Only skip if it's a single word matching county
            if len(p_lower.split()) == 1:
                continue

        # Skip segments starting with house/apt numbers ("5 Maypark Mews", "Apt 3")
        if _HOUSE_NUM_RE.match(part):
            continue

        # Skip Irish Eircodes (e.g. "D15 FH01", "X91DWX8")
        if _EIRCODE_RE.match(part):
            continue

        # Skip property reference codes from estate agents (e.g. "RN715F")
        if _PROP_REF_RE.match(part):
            continue

        candidates.append(part)

    if not candidates:
        # Fallback: if everything was filtered, return first non-county segment or area_text
        for part in parts:
            p_lower = part.lower().strip()
            if _COUNTY_RE.match(p_lower):
                continue
            return part.strip().title()
        return area_text.strip()

    # Prefer named areas over Dublin postal codes (e.g., "Rathmines" > "Dublin 6")
    non_postal = [c for c in candidates if not _DUBLIN_POSTAL_RE.match(c)]
    if non_postal:
        return non_postal[-1].strip().title()

    # Only postal codes remain — return the last one (e.g., "Dublin 8")
    return candidates[-1].strip().title()


# Regex to extract beds from title/property_type (e.g. "3 Bed Apartment", "2 bedroom")
_BEDS_FROM_TITLE_RE = re.compile(r"(\d+)\s*(?:bed|bedroom)", re.IGNORECASE)


def _extract_beds_from_title(title: str | None) -> int | None:
    """Extract bed count from title or property_type string as fallback."""
    if not title:
        return None
    match = _BEDS_FROM_TITLE_RE.search(title)
    return int(match.group(1)) if match else None


# ═══════════════════════════════════════════════════════════════════════════════
# UNION-FIND DEDUP CLUSTERING
# ═══════════════════════════════════════════════════════════════════════════════


class UnionFind:
    """Disjoint-set (Union-Find) with path compression."""

    def __init__(self):
        self.parent: dict[int, int] = {}

    def find(self, x: int) -> int:
        if x not in self.parent:
            self.parent[x] = x
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def build_clusters(
    pairs: list[tuple[int, int]],
    max_cluster_size: int = 6,
) -> dict[int, int]:
    """
    Build cluster_id mapping from listing-link pairs.

    Args:
        pairs: list of (raw_id_a, raw_id_b) tuples
        max_cluster_size: maximum allowed cluster size (default 6 = one per source)

    Returns:
        {raw_id: cluster_id} for every raw_id that appears in at least one pair.
        cluster_id is the canonical root of the Union-Find set.
    """
    if not pairs:
        return {}

    uf = UnionFind()
    cluster_sizes: dict[int, int] = {}  # root -> size

    for a, b in pairs:
        ra, rb = uf.find(a), uf.find(b)
        if ra == rb:
            continue  # already in same cluster

        size_a = cluster_sizes.get(ra, 1)
        size_b = cluster_sizes.get(rb, 1)

        if size_a + size_b > max_cluster_size:
            continue  # skip merge — would exceed cap

        uf.union(a, b)
        new_root = uf.find(a)
        cluster_sizes[new_root] = size_a + size_b

    # Collect all members and map to their root
    members: set[int] = set()
    for a, b in pairs:
        members.add(a)
        members.add(b)

    return {m: uf.find(m) for m in members}


def _assign_cluster_ids(session) -> int:
    """
    Read listing_links, compute clusters, write cluster_id to pub.public_listings.

    Returns:
        Number of listings updated with a cluster_id.
    """
    # 1. Clear old cluster IDs (re-compute every sync)
    session.execute(text(
        "UPDATE pub.public_listings SET cluster_id = NULL WHERE cluster_id IS NOT NULL"
    ))

    # 2. Read all pairs
    rows = session.execute(text(
        "SELECT raw_id_a, raw_id_b FROM core.listing_links"
    )).fetchall()

    pairs = [(r.raw_id_a, r.raw_id_b) for r in rows]
    if not pairs:
        return 0

    # 3. Build clusters
    clusters = build_clusters(pairs)

    # 4. Write cluster_id
    updated = 0
    for raw_id, cluster_id in clusters.items():
        result = session.execute(text(
            "UPDATE pub.public_listings SET cluster_id = :cid "
            "WHERE raw_id = :rid AND status = 'active'"
        ), {"cid": cluster_id, "rid": raw_id})
        updated += result.rowcount

    logger.info("Cluster IDs assigned", pairs=len(pairs), clusters_updated=updated)
    return updated


# ═══════════════════════════════════════════════════════════════════════════════
# PUB SYNC
# ═══════════════════════════════════════════════════════════════════════════════


def sync_listings_to_pub(
    since: datetime | None = None,
    batch_size: int = 100,
    source_filter: str | None = None,
) -> dict:
    """
    Sync normalized listings to pub.listings_current.

    This provides a read-optimized view for the bot/notifications.

    Args:
        since: Only sync listings updated since this time
        batch_size: Number of listings to process per batch
        source_filter: Optional source name to filter (e.g. 'daft')

    Returns:
        Stats dict with counts
    """
    stats = {
        "processed": 0,
        "inserted": 0,
        "updated": 0,
        "errors": 0,
    }

    try:
        with get_session_context() as session:
            # Get listings that need sync
            query = """
                SELECT
                    n.id as norm_id,
                    n.raw_id,
                    n.price,
                    n.beds,
                    n.baths,
                    n.property_type,
                    n.city_id,
                    n.county,
                    n.area_text,
                    n.lease_length_months,
                    n.lease_length_unknown,
                    n.status,
                    n.updated_at,
                    r.source,
                    r.source_listing_id,
                    r.url,
                    r.first_photo_url,
                    r.first_seen
                FROM core.listings_normalized n
                JOIN raw.listings_raw r ON r.id = n.raw_id
                WHERE n.status = 'active'
            """

            params: dict[str, Any] = {}
            if since:
                query += " AND n.updated_at > :since"
                params["since"] = since
            if source_filter:
                query += " AND r.source = :source"
                params["source"] = source_filter

            query += " ORDER BY n.updated_at LIMIT :batch_size OFFSET :offset"

            # Process in batches to avoid loading all into memory
            offset = 0
            params["batch_size"] = batch_size
            params["offset"] = offset

            while True:
                params["offset"] = offset
                result = session.execute(text(query), params)
                rows = result.fetchall()

                if not rows:
                    break  # No more rows

                for row in rows:
                    try:
                        action = _upsert_to_pub(session, row)
                        stats["processed"] += 1
                        if action == "insert":
                            stats["inserted"] += 1
                        else:
                            stats["updated"] += 1
                    except Exception as e:
                        logger.error("Failed to sync listing", error=str(e))
                        stats["errors"] += 1

                # Commit each batch
                session.commit()

                if len(rows) < batch_size:
                    break  # Last batch

                offset += batch_size
                logger.debug("Batch processed", offset=offset, batch_size=batch_size)

    except Exception as e:
        logger.error("Pub sync failed", error=str(e))
        raise

    # Assign dedup cluster IDs from core.listing_links
    try:
        with get_session_context() as session:
            n_clustered = _assign_cluster_ids(session)
            session.commit()
            stats["clustered"] = n_clustered
    except Exception as e:
        logger.warning("Cluster assignment failed", error=str(e))

    # Refresh city stats materialized view
    try:
        with get_session_context() as session:
            session.execute(text(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY pub.city_stats"
            ))
            session.commit()
            logger.info("City stats view refreshed")
    except Exception as e:
        # View may not exist yet — log warning but don't fail sync
        logger.warning("City stats view refresh failed (may not exist yet)", error=str(e))

    logger.info("Pub sync completed", **stats)
    return stats


def _upsert_to_pub(session: Any, row: Any) -> str:
    """
    Upsert a single listing to pub.public_listings.

    Returns:
        'insert' if new row, 'update' if existing row updated
    """
    query = """
        INSERT INTO pub.public_listings (
            raw_id,
            source,
            url,
            first_photo_url,
            price,
            beds,
            baths,
            property_type,
            county,
            city,
            area_text,
            published_at,
            updated_at,
            status
        ) VALUES (
            :raw_id,
            :source,
            :url,
            :first_photo_url,
            :price,
            :beds,
            :baths,
            :property_type,
            :county,
            :city,
            :area_text,
            :published_at,
            :updated_at,
            :status
        )
        ON CONFLICT (raw_id)
        DO UPDATE SET
            price = EXCLUDED.price,
            beds = EXCLUDED.beds,
            baths = EXCLUDED.baths,
            property_type = EXCLUDED.property_type,
            county = EXCLUDED.county,
            city = EXCLUDED.city,
            area_text = EXCLUDED.area_text,
            updated_at = NOW(),
            status = EXCLUDED.status
        RETURNING (xmax = 0) AS was_insert
    """

    # Handle property_type enum
    prop_type = row.property_type
    if hasattr(prop_type, "value"):
        prop_type = prop_type.value

    result = session.execute(text(query), {
        "raw_id": row.raw_id,
        "source": row.source,
        "url": row.url,
        "first_photo_url": row.first_photo_url,
        "price": row.price,
        "beds": row.beds or _extract_beds_from_title(getattr(row, 'title', None) or prop_type),
        "baths": row.baths,
        "property_type": prop_type,
        "county": row.county,
        "city": parse_city_from_area(row.area_text, row.county),
        "area_text": row.area_text,
        "published_at": row.first_seen,  # Use first_seen from raw per ARCHITECT.md
        "updated_at": datetime.now(UTC),
        "status": row.status.value if hasattr(row.status, "value") else row.status,
    })

    was_insert = result.scalar()
    return "insert" if was_insert else "update"


# ═══════════════════════════════════════════════════════════════════════════════
# REMOVAL SYNC
# ═══════════════════════════════════════════════════════════════════════════════


def sync_removed_listings() -> dict:
    """
    Mark listings as removed in pub if they're gone from core.

    Returns:
        Stats dict
    """
    stats = {"marked_removed": 0}

    try:
        with get_session_context() as session:
            query = """
                UPDATE pub.public_listings pub
                SET
                    status = 'removed',
                    updated_at = NOW()
                FROM core.listings_normalized n
                WHERE pub.raw_id = n.raw_id
                AND n.status = 'removed'
                AND pub.status != 'removed'
            """

            result = session.execute(text(query))
            stats["marked_removed"] = result.rowcount
            session.commit()

    except Exception as e:
        logger.error("Removal sync failed", error=str(e))
        raise

    return stats


def get_pub_sync_stats() -> dict:
    """Get statistics about pub schema."""
    try:
        with get_session_context() as session:
            result = session.execute(text("""
                SELECT
                    status,
                    COUNT(*) as count
                FROM pub.public_listings
                GROUP BY status
            """))

            status_counts = {row.status: row.count for row in result}

            result = session.execute(text("""
                SELECT
                    source,
                    COUNT(*) as count
                FROM pub.public_listings
                WHERE status = 'active'
                GROUP BY source
            """))

            source_counts = {row.source: row.count for row in result}

            return {
                "by_status": status_counts,
                "active_by_source": source_counts,
            }

    except Exception as e:
        logger.error("Failed to get pub stats", error=str(e))
        return {}
