"""add document processing runs

Revision ID: 20260502_0007
Revises: 20260502_0006
Create Date: 2026-05-02 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260502_0007"
down_revision = "20260502_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "document_processing_runs",
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("total_duration_ms", sa.Integer(), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("error_stage", sa.String(length=100), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("stage_metrics_json", postgresql.JSONB(), nullable=False),
        sa.Column("summary_metrics_json", postgresql.JSONB(), nullable=False),
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
        sa.ForeignKeyConstraint(["document_id"], ["uploaded_documents.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_document_processing_runs_document_id"),
        "document_processing_runs",
        ["document_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_document_processing_runs_status"),
        "document_processing_runs",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_document_processing_runs_status"),
        table_name="document_processing_runs",
    )
    op.drop_index(
        op.f("ix_document_processing_runs_document_id"),
        table_name="document_processing_runs",
    )
    op.drop_table("document_processing_runs")
