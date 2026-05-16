"""add document processing pipeline

Revision ID: 20260502_0005
Revises: 20260421_0004
Create Date: 2026-05-02 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql


revision = "20260502_0005"
down_revision = "20260421_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute("CREATE EXTENSION IF NOT EXISTS unaccent")

    op.add_column(
        "uploaded_documents",
        sa.Column(
            "processing_status",
            sa.String(length=30),
            nullable=False,
            server_default="not_started",
        ),
    )
    op.add_column("uploaded_documents", sa.Column("processing_error", sa.Text(), nullable=True))
    op.add_column(
        "uploaded_documents",
        sa.Column("processing_error_code", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "uploaded_documents",
        sa.Column("processing_error_stage", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "uploaded_documents",
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "uploaded_documents",
        sa.Column("processing_completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column("uploaded_documents", sa.Column("parser_version", sa.String(length=100), nullable=True))
    op.add_column(
        "uploaded_documents",
        sa.Column("extraction_version", sa.String(length=100), nullable=True),
    )
    op.create_index(
        op.f("ix_uploaded_documents_processing_status"),
        "uploaded_documents",
        ["processing_status"],
        unique=False,
    )

    op.create_table(
        "parsed_documents",
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("raw_text", sa.Text(), nullable=False),
        sa.Column("language", sa.String(length=50), nullable=False),
        sa.Column("parser_version", sa.String(length=100), nullable=False),
        sa.Column("page_count", sa.Integer(), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(), nullable=True),
        sa.Column("outline_json", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["document_id"], ["uploaded_documents.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("document_id"),
    )
    op.create_index(
        op.f("ix_parsed_documents_document_id"),
        "parsed_documents",
        ["document_id"],
        unique=False,
    )

    op.create_table(
        "document_chunks",
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunk_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=False),
        sa.Column("section_path", sa.String(length=1000), nullable=True),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("fts_text", sa.Text(), nullable=False),
        sa.Column("search_vector", postgresql.TSVECTOR(), nullable=True),
        sa.Column("embedding", Vector(1536), nullable=True),
        sa.Column("embedding_model", sa.String(length=100), nullable=True),
        sa.Column("token_count", sa.Integer(), nullable=False),
        sa.Column("page_start", sa.Integer(), nullable=True),
        sa.Column("page_end", sa.Integer(), nullable=True),
        sa.Column("char_start", sa.Integer(), nullable=False),
        sa.Column("char_end", sa.Integer(), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["document_id"], ["uploaded_documents.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("chunk_id"),
    )
    op.create_index(op.f("ix_document_chunks_document_id"), "document_chunks", ["document_id"])
    op.create_index(op.f("ix_document_chunks_chunk_id"), "document_chunks", ["chunk_id"], unique=True)
    op.create_index(
        "ix_document_chunks_search_vector",
        "document_chunks",
        ["search_vector"],
        postgresql_using="gin",
    )
    op.create_index(
        "ix_document_chunks_fts_text_trgm",
        "document_chunks",
        ["fts_text"],
        postgresql_using="gin",
        postgresql_ops={"fts_text": "gin_trgm_ops"},
    )

    op.create_table(
        "document_extraction_items",
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunk_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("type", sa.String(length=100), nullable=False),
        sa.Column("value_json", postgresql.JSONB(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("source", sa.String(length=100), nullable=False),
        sa.Column("evidence_text", sa.Text(), nullable=True),
        sa.Column("page_start", sa.Integer(), nullable=True),
        sa.Column("page_end", sa.Integer(), nullable=True),
        sa.Column("char_start", sa.Integer(), nullable=True),
        sa.Column("char_end", sa.Integer(), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["chunk_id"], ["document_chunks.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["document_id"], ["uploaded_documents.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_document_extraction_items_document_id"),
        "document_extraction_items",
        ["document_id"],
    )
    op.create_index(
        op.f("ix_document_extraction_items_chunk_id"),
        "document_extraction_items",
        ["chunk_id"],
    )
    op.create_index(
        op.f("ix_document_extraction_items_type"),
        "document_extraction_items",
        ["type"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_document_extraction_items_type"), table_name="document_extraction_items")
    op.drop_index(op.f("ix_document_extraction_items_chunk_id"), table_name="document_extraction_items")
    op.drop_index(op.f("ix_document_extraction_items_document_id"), table_name="document_extraction_items")
    op.drop_table("document_extraction_items")
    op.drop_index("ix_document_chunks_fts_text_trgm", table_name="document_chunks")
    op.drop_index("ix_document_chunks_search_vector", table_name="document_chunks")
    op.drop_index(op.f("ix_document_chunks_chunk_id"), table_name="document_chunks")
    op.drop_index(op.f("ix_document_chunks_document_id"), table_name="document_chunks")
    op.drop_table("document_chunks")
    op.drop_index(op.f("ix_parsed_documents_document_id"), table_name="parsed_documents")
    op.drop_table("parsed_documents")
    op.drop_index(op.f("ix_uploaded_documents_processing_status"), table_name="uploaded_documents")
    op.drop_column("uploaded_documents", "extraction_version")
    op.drop_column("uploaded_documents", "parser_version")
    op.drop_column("uploaded_documents", "processing_completed_at")
    op.drop_column("uploaded_documents", "processing_started_at")
    op.drop_column("uploaded_documents", "processing_error_stage")
    op.drop_column("uploaded_documents", "processing_error_code")
    op.drop_column("uploaded_documents", "processing_error")
    op.drop_column("uploaded_documents", "processing_status")
