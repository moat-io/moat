from database import BaseModel, DateTimeUTC
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Mapped
from datetime import datetime


class IngestionProcessDbo(BaseModel):
    __tablename__ = "ingestion_processes"

    ingestion_process_id: Mapped[int] = Column(
        Integer, primary_key=True, autoincrement=True
    )
    source: Mapped[str] = Column(String(255))  # trino, postgres, ldap etc
    object_type: Mapped[str] = Column(String(64))  # tag, object, principal, group
    started_at: Mapped[datetime] = Column(DateTimeUTC(timezone=True))
    completed_at: Mapped[datetime] = Column(DateTimeUTC(timezone=True))
    status: Mapped[str] = Column(String(50), default="running")
    log: Mapped[str] = Column(String(4096))
