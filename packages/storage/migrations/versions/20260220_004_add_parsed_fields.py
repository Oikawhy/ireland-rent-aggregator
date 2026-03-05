"""Add parsed text columns to raw.listings_raw

These columns store adapter-extracted fields (beds_text, title, price_text, etc.)
that the normalizer needs for direct access instead of parsing raw_payload JSON.

Revision ID: 004_add_parsed_fields
Revises: 003_add_cluster_id
Create Date: 2026-02-20
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "004_add_parsed_fields"
down_revision: Union[str, None] = "003_add_cluster_id"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("listings_raw", sa.Column("title", sa.String(500), nullable=True), schema="raw")
    op.add_column("listings_raw", sa.Column("price_text", sa.String(100), nullable=True), schema="raw")
    op.add_column("listings_raw", sa.Column("beds_text", sa.String(50), nullable=True), schema="raw")
    op.add_column("listings_raw", sa.Column("baths_text", sa.String(50), nullable=True), schema="raw")
    op.add_column("listings_raw", sa.Column("property_type_text", sa.String(100), nullable=True), schema="raw")
    op.add_column("listings_raw", sa.Column("location_text", sa.String(500), nullable=True), schema="raw")
    op.add_column("listings_raw", sa.Column("description", sa.Text(), nullable=True), schema="raw")


def downgrade() -> None:
    op.drop_column("listings_raw", "description", schema="raw")
    op.drop_column("listings_raw", "location_text", schema="raw")
    op.drop_column("listings_raw", "property_type_text", schema="raw")
    op.drop_column("listings_raw", "baths_text", schema="raw")
    op.drop_column("listings_raw", "beds_text", schema="raw")
    op.drop_column("listings_raw", "price_text", schema="raw")
    op.drop_column("listings_raw", "title", schema="raw")
