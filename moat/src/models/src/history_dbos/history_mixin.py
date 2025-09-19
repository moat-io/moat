from sqlalchemy import Column, DateTime, Integer, String, JSON, BigInteger, text
from sqlalchemy.sql.functions import current_timestamp
from sqlalchemy.orm import declared_attr, DeclarativeBase, Mapped, mapped_column


class HistoryMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    history_id: Mapped[str] = mapped_column(
        String, primary_key=True, server_default=text("gen_random_uuid()")
    )
    history_record_created_date: Mapped[str] = mapped_column(
        DateTime(timezone=True), server_default=current_timestamp()
    )
    history_change_operation: Mapped[str] = mapped_column(String(50))
