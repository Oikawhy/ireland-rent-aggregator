"""Add cluster_id column to pub.public_listings for cross-source deduplication

Revision ID: 003_add_cluster_id
Revises: 002_add_city_stats
Create Date: 2026-02-20
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "003_add_cluster_id"
down_revision: Union[str, None] = "002_add_city_stats"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "public_listings",
        sa.Column("cluster_id", sa.Integer(), nullable=True),
        schema="pub",
    )
    op.create_index(
        "idx_public_listings_cluster_id",
        "public_listings",
        ["cluster_id"],
        schema="pub",
    )


def downgrade() -> None:
    op.drop_index(
        "idx_public_listings_cluster_id",
        table_name="public_listings",
        schema="pub",
    )
    op.drop_column("public_listings", "cluster_id", schema="pub")
