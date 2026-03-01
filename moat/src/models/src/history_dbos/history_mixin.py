from datetime import datetime
from database import DateTimeUTC
from sqlalchemy import String, func
from sqlalchemy.orm import declared_attr, Mapped, mapped_column


class HistoryMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    history_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    history_record_created_date: Mapped[datetime] = mapped_column(
        DateTimeUTC(timezone=True), server_default=func.now()
    )
    history_change_operation: Mapped[str] = mapped_column(String(50))
