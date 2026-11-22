from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped

from database import BaseModel


class RoleDbo(BaseModel):
    __tablename__ = "roles"

    policy_evaluator_result_id: Mapped[int] = Column(
        Integer,
        ForeignKey("policy_evaluator_results.id", ondelete="CASCADE"),
        primary_key=True,
    )
    role_id: Mapped[str] = Column(String(256), nullable=False, index=True)
