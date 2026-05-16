"""create institutions

Revision ID: 20260502_0009
Revises: 20260502_0008
Create Date: 2026-05-02 00:00:00
"""

from datetime import datetime, timezone
from uuid import UUID

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260502_0009"
down_revision = "20260502_0008"
branch_labels = None
depends_on = None


INSTITUTIONS = (
    ("00000000-0000-0000-0000-000000000101", "ІАДУ", "Адміністрування, державного управління та професійного розвитку"),
    ("00000000-0000-0000-0000-000000000102", "ІАРД", "Архітектура та дизайн"),
    ("00000000-0000-0000-0000-000000000103", "ІБІБ", "Будівництво, інфраструктура та безпека життєдіяльності"),
    ("00000000-0000-0000-0000-000000000104", "ІГДГ", "Геодезія"),
    ("00000000-0000-0000-0000-000000000105", "ІГСН", "Гуманітарні та соціальні науки"),
    ("00000000-0000-0000-0000-000000000106", "ІНЕМ", "Економіка і менеджмент"),
    ("00000000-0000-0000-0000-000000000107", "ІЕСК", "Енергетика та системи керування"),
    ("00000000-0000-0000-0000-000000000108", "ІКТЕ", "Інформаційно-комунікаційних технологій та електронної інженерії"),
    ("00000000-0000-0000-0000-000000000109", "ІКНІ", "Комп'ютерні науки та інформаційні технології"),
    ("00000000-0000-0000-0000-000000000110", "ІКТА", "Комп'ютерні технології, автоматика та метрологія"),
    ("00000000-0000-0000-0000-000000000111", "ІМІТ", "Механічна інженерія та транспорт"),
    ("00000000-0000-0000-0000-000000000112", "ІПМТ", "Поліграфії та медійних технологій"),
    ("00000000-0000-0000-0000-000000000113", "ІППТ", "Просторове планування та перспективні технології"),
    ("00000000-0000-0000-0000-000000000114", "ІППО", "Право, психологія та інноваційна освіта"),
    ("00000000-0000-0000-0000-000000000115", "ІМФН", "Прикладна математика та фундаментальні науки"),
    ("00000000-0000-0000-0000-000000000116", "ІСТР", "Сталий розвиток"),
    ("00000000-0000-0000-0000-000000000117", "ІХХТ", "Хімія та хімічні технології"),
    ("00000000-0000-0000-0000-000000000118", "МІОК", "Освіта, культура та зв'язки з діаспорою"),
)


def upgrade() -> None:
    op.create_table(
        "institutions",
        sa.Column("code", sa.String(length=20), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
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
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code"),
        sa.UniqueConstraint("name"),
    )
    op.create_index(op.f("ix_institutions_code"), "institutions", ["code"])

    institutions_table = sa.table(
        "institutions",
        sa.column("id", postgresql.UUID(as_uuid=True)),
        sa.column("code", sa.String),
        sa.column("name", sa.String),
        sa.column("sort_order", sa.Integer),
        sa.column("is_active", sa.Boolean),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    now = datetime(2026, 5, 2, tzinfo=timezone.utc)
    op.bulk_insert(
        institutions_table,
        [
            {
                "id": UUID(institution_id),
                "code": code,
                "name": name,
                "sort_order": index,
                "is_active": True,
                "created_at": now,
                "updated_at": now,
            }
            for index, (institution_id, code, name) in enumerate(INSTITUTIONS, start=1)
        ],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_institutions_code"), table_name="institutions")
    op.drop_table("institutions")
