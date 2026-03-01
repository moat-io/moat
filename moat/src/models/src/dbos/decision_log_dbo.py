from datetime import datetime

from database import BaseModel, DateTimeUTC
from sqlalchemy import Boolean, Column, DateTime, String
from sqlalchemy.orm import Mapped


class DecisionLogDbo(BaseModel):
    __tablename__ = "decision_logs"

    @property
    def f_q_object_name(self) -> str:
        return ".".join(
            f for f in [self.database, self.schema, self.table, self.column] if f
        )

    decision_log_id: Mapped[str] = Column(String(255), primary_key=True)
    path: Mapped[str] = Column(String(1024))
    operation: Mapped[str] = Column(String(50))
    username: Mapped[str] = Column(String(255))
    database: Mapped[str] = Column(String(255))
    schema: Mapped[str] = Column(String(255))
    table: Mapped[str] = Column(String(255))
    column: Mapped[str] = Column(String(255))
    permitted: Mapped[bool] = Column(Boolean)
    expression: Mapped[str] = Column(String(2048))
    timestamp: Mapped[datetime] = Column(DateTimeUTC(timezone=True))
