from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class Person(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "people"

    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    position: Mapped[str] = mapped_column(String(255), nullable=True)
    department: Mapped[str] = mapped_column(String(255), nullable=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
