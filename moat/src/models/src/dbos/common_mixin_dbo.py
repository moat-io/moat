from datetime import datetime
from sqlalchemy import Boolean, Column, Integer, DateTime
from sqlalchemy.orm import declared_attr, Mapped
from sqlalchemy.sql.functions import current_timestamp


class IngestionDboMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    # Note: original comment said these cannot be type-hinted, but we're adding Mapped[] as required
    ingestion_process_id: Mapped[int] = Column(Integer)
    active: Mapped[bool] = Column(Boolean, default=True, server_default="t")


class MetadataDboMixin:
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    record_updated_date: Mapped[datetime] = Column(
        DateTime(timezone=True),
        server_default=current_timestamp(),
        server_onupdate=current_timestamp(),
    )

    record_created_date: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=current_timestamp()
    )
