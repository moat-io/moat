from datetime import datetime
from database import DateTimeUTC
from sqlalchemy import Boolean, Column, Integer, func
from sqlalchemy.orm import declared_attr, Mapped
from sqlalchemy.sql import expression


class IngestionDboMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    ingestion_process_id: Mapped[int] = Column(Integer)
    active: Mapped[bool] = Column(
        Boolean, default=True, server_default=expression.true()
    )


class MetadataDboMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    record_updated_date: Mapped[datetime] = Column(
        DateTimeUTC(timezone=True),
        server_default=func.now(),
        server_onupdate=func.now(),
    )

    record_created_date: Mapped[datetime] = Column(
        DateTimeUTC(timezone=True), server_default=func.now()
    )
