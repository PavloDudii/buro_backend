"""add document upload intents

Revision ID: 20260502_0008
Revises: 20260502_0007
Create Date: 2026-05-02 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260502_0008"
down_revision = "20260502_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "document_upload_intents",
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("uploaded_by_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("original_filename", sa.String(length=500), nullable=False),
        sa.Column("safe_filename", sa.String(length=500), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=False),
        sa.Column("file_extension", sa.String(length=20), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256_hash", sa.String(length=64), nullable=True),
        sa.Column("planned_pathname", sa.String(length=1000), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("blob_url", sa.String(length=2000), nullable=True),
        sa.Column("blob_download_url", sa.String(length=2000), nullable=True),
        sa.Column("blob_etag", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["uploaded_by_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("document_id"),
        sa.UniqueConstraint("planned_pathname"),
    )
    op.create_index(
        op.f("ix_document_upload_intents_uploaded_by_id"),
        "document_upload_intents",
        ["uploaded_by_id"],
    )
    op.create_index(
        op.f("ix_document_upload_intents_status"),
        "document_upload_intents",
        ["status"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_document_upload_intents_status"), table_name="document_upload_intents")
    op.drop_index(
        op.f("ix_document_upload_intents_uploaded_by_id"),
        table_name="document_upload_intents",
    )
    op.drop_table("document_upload_intents")
