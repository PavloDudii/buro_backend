"""create programs

Revision ID: 20260502_0011
Revises: 20260502_0010
Create Date: 2026-05-02 00:11:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260502_0011"
down_revision: str | None = "20260502_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "education_programs",
        sa.Column("level", sa.String(length=30), nullable=False),
        sa.Column("field_code", sa.String(length=50), nullable=False),
        sa.Column("field_name", sa.String(length=500), nullable=False),
        sa.Column("specialty_code", sa.String(length=50), nullable=False),
        sa.Column("specialty_name", sa.String(length=500), nullable=False),
        sa.Column("program_name", sa.String(length=1000), nullable=False),
        sa.Column("qualification", sa.String(length=1000), nullable=True),
        sa.Column("study_form", sa.String(length=255), nullable=True),
        sa.Column("duration", sa.String(length=255), nullable=True),
        sa.Column("credits", sa.String(length=255), nullable=True),
        sa.Column("manager", sa.String(length=500), nullable=True),
        sa.Column("program_url", sa.String(length=2000), nullable=True),
        sa.Column("source_page_url", sa.String(length=2000), nullable=False),
        sa.Column("institution_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("department_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("department_link_status", sa.String(length=30), nullable=False),
        sa.Column("department_match_confidence", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["department_id"], ["departments.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["institution_id"], ["institutions.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("level", "specialty_code", "program_name"),
    )
    op.create_index(op.f("ix_education_programs_level"), "education_programs", ["level"])
    op.create_index(
        op.f("ix_education_programs_field_code"),
        "education_programs",
        ["field_code"],
    )
    op.create_index(
        op.f("ix_education_programs_specialty_code"),
        "education_programs",
        ["specialty_code"],
    )
    op.create_index(
        op.f("ix_education_programs_program_name"),
        "education_programs",
        ["program_name"],
    )
    op.create_index(
        op.f("ix_education_programs_institution_id"),
        "education_programs",
        ["institution_id"],
    )
    op.create_index(
        op.f("ix_education_programs_department_id"),
        "education_programs",
        ["department_id"],
    )
    op.create_index(
        op.f("ix_education_programs_department_link_status"),
        "education_programs",
        ["department_link_status"],
    )

    op.create_table(
        "program_documents",
        sa.Column("program_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("uploaded_document_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_url", sa.String(length=2000), nullable=False),
        sa.Column("title", sa.String(length=1000), nullable=False),
        sa.Column("kind", sa.String(length=100), nullable=False),
        sa.Column("source_size_label", sa.String(length=100), nullable=True),
        sa.Column("source_size_bytes", sa.Integer(), nullable=True),
        sa.Column("import_status", sa.String(length=30), nullable=False),
        sa.Column("import_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["program_id"],
            ["education_programs.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["uploaded_document_id"],
            ["uploaded_documents.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("program_id", "source_url"),
    )
    op.create_index(op.f("ix_program_documents_program_id"), "program_documents", ["program_id"])
    op.create_index(
        op.f("ix_program_documents_uploaded_document_id"),
        "program_documents",
        ["uploaded_document_id"],
    )
    op.create_index(op.f("ix_program_documents_kind"), "program_documents", ["kind"])
    op.create_index(
        op.f("ix_program_documents_import_status"),
        "program_documents",
        ["import_status"],
    )

    op.create_table(
        "program_import_runs",
        sa.Column("source_url", sa.String(length=2000), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("program_count", sa.Integer(), nullable=False),
        sa.Column("created_document_count", sa.Integer(), nullable=False),
        sa.Column("oversized_document_count", sa.Integer(), nullable=False),
        sa.Column("failed_document_count", sa.Integer(), nullable=False),
        sa.Column("matched_program_count", sa.Integer(), nullable=False),
        sa.Column("pending_review_program_count", sa.Integer(), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_program_import_runs_status"), "program_import_runs", ["status"])


def downgrade() -> None:
    op.drop_index(op.f("ix_program_import_runs_status"), table_name="program_import_runs")
    op.drop_table("program_import_runs")
    op.drop_index(op.f("ix_program_documents_import_status"), table_name="program_documents")
    op.drop_index(op.f("ix_program_documents_kind"), table_name="program_documents")
    op.drop_index(
        op.f("ix_program_documents_uploaded_document_id"),
        table_name="program_documents",
    )
    op.drop_index(op.f("ix_program_documents_program_id"), table_name="program_documents")
    op.drop_table("program_documents")
    op.drop_index(
        op.f("ix_education_programs_department_link_status"),
        table_name="education_programs",
    )
    op.drop_index(op.f("ix_education_programs_department_id"), table_name="education_programs")
    op.drop_index(op.f("ix_education_programs_institution_id"), table_name="education_programs")
    op.drop_index(op.f("ix_education_programs_program_name"), table_name="education_programs")
    op.drop_index(op.f("ix_education_programs_specialty_code"), table_name="education_programs")
    op.drop_index(op.f("ix_education_programs_field_code"), table_name="education_programs")
    op.drop_index(op.f("ix_education_programs_level"), table_name="education_programs")
    op.drop_table("education_programs")
