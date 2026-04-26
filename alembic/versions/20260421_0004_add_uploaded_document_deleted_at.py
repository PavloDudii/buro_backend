"""add uploaded document deleted_at

Revision ID: 20260421_0004
Revises: 20260421_0003
Create Date: 2026-04-21 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260421_0004"
down_revision = "20260421_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "uploaded_documents",
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("uploaded_documents", "deleted_at")
