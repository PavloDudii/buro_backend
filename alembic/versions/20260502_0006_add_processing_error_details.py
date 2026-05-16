"""add processing error details

Revision ID: 20260502_0006
Revises: 20260502_0005
Create Date: 2026-05-02 00:00:00
"""

from alembic import op


revision = "20260502_0006"
down_revision = "20260502_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE uploaded_documents "
        "ADD COLUMN IF NOT EXISTS processing_error_code varchar(100)"
    )
    op.execute(
        "ALTER TABLE uploaded_documents "
        "ADD COLUMN IF NOT EXISTS processing_error_stage varchar(100)"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE uploaded_documents DROP COLUMN IF EXISTS processing_error_stage")
    op.execute("ALTER TABLE uploaded_documents DROP COLUMN IF EXISTS processing_error_code")
