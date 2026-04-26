"""create uploaded documents table

Revision ID: 20260421_0003
Revises: 20260421_0002
Create Date: 2026-04-21 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260421_0003"
down_revision = "20260421_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "uploaded_documents",
        sa.Column("original_filename", sa.String(length=500), nullable=False),
        sa.Column("safe_filename", sa.String(length=500), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=False),
        sa.Column("file_extension", sa.String(length=20), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256_hash", sa.String(length=64), nullable=False),
        sa.Column("storage_key", sa.String(length=1000), nullable=True),
        sa.Column("uploaded_by_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["uploaded_by_id"], ["users.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_uploaded_documents_sha256_hash"),
        "uploaded_documents",
        ["sha256_hash"],
        unique=False,
    )
    op.create_index(
        op.f("ix_uploaded_documents_uploaded_by_id"),
        "uploaded_documents",
        ["uploaded_by_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_uploaded_documents_uploaded_by_id"), table_name="uploaded_documents")
    op.drop_index(op.f("ix_uploaded_documents_sha256_hash"), table_name="uploaded_documents")
    op.drop_table("uploaded_documents")
