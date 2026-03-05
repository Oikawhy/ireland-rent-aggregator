"""Add pub.city_stats materialized view

Revision ID: 002_add_city_stats
Revises: 001_initial_schema
Create Date: 2026-02-20
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "002_add_city_stats"
down_revision: Union[str, None] = "001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS pub.city_stats AS
        SELECT
            city,
            county,
            COUNT(DISTINCT COALESCE(cluster_id, listing_id)) AS listing_count,
            ROUND(AVG(price)::numeric, 0) AS avg_price,
            MIN(price) AS min_price,
            MAX(price) AS max_price,
            NOW() AS refreshed_at
        FROM pub.public_listings
        WHERE status = 'active' AND city IS NOT NULL
        GROUP BY city, county;
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_city_stats_city_county
            ON pub.city_stats (city, county);
    """)


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS pub.city_stats;")
