from datetime import datetime

from database import BaseModel
from sqlalchemy import Boolean, Column, DateTime, String
from sqlalchemy.orm import Mapped


class DecisionLogDbo(BaseModel):
    __tablename__ = "decision_logs"

    @property
    def f_q_object_name(self) -> str:
        return ".".join(
            f for f in [self.database, self.schema, self.table, self.column] if f
        )

    decision_log_id: Mapped[str] = Column(String, primary_key=True)
    path: Mapped[str] = Column(String)
    operation: Mapped[str] = Column(String)
    username: Mapped[str] = Column(String)
    database: Mapped[str] = Column(String)
    schema: Mapped[str] = Column(String)
    table: Mapped[str] = Column(String)
    column: Mapped[str] = Column(String)
    permitted: Mapped[bool] = Column(Boolean)
    expression: Mapped[str] = Column(String)
    timestamp: Mapped[datetime] = Column(DateTime(timezone=True))
