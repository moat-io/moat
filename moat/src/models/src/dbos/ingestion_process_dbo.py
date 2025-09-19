from database import BaseModel
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import Mapped
from datetime import datetime


class IngestionProcessDbo(BaseModel):
    __tablename__ = "ingestion_processes"

    ingestion_process_id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = Column(String)  # trino, postgres, ldap etc
    object_type: Mapped[str] = Column(String)  # tag, object, principal, group
    started_at: Mapped[datetime] = Column(DateTime(timezone=True))
    completed_at: Mapped[datetime] = Column(DateTime(timezone=True))
    status: Mapped[str] = Column(String, default="running")
    log: Mapped[str] = Column(String)
