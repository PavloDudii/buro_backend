"""add program directory snapshots

Revision ID: 20260506_0012
Revises: 20260502_0011
Create Date: 2026-05-06 00:12:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260506_0012"
down_revision: str | None = "20260502_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "education_programs",
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        op.f("ix_education_programs_deleted_at"),
        "education_programs",
        ["deleted_at"],
    )
    op.create_table(
        "program_directory_snapshots",
        sa.Column("program_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_url", sa.String(length=2000), nullable=False),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("raw_text", sa.Text(), nullable=False),
        sa.Column("structured_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("sections_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("parsed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["program_id"],
            ["education_programs.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("program_id", "source_url"),
    )
    op.create_index(
        op.f("ix_program_directory_snapshots_program_id"),
        "program_directory_snapshots",
        ["program_id"],
    )
    op.create_index(
        op.f("ix_program_directory_snapshots_year"),
        "program_directory_snapshots",
        ["year"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_program_directory_snapshots_year"), table_name="program_directory_snapshots")
    op.drop_index(
        op.f("ix_program_directory_snapshots_program_id"),
        table_name="program_directory_snapshots",
    )
    op.drop_table("program_directory_snapshots")
    op.drop_index(op.f("ix_education_programs_deleted_at"), table_name="education_programs")
    op.drop_column("education_programs", "deleted_at")
