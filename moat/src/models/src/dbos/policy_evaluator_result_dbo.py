from database import BaseModel
from sqlalchemy import Column, Integer, Text
from sqlalchemy.orm import Mapped


class PolicyEvaluatorResultDbo(BaseModel):
    __tablename__ = "policy_evaluator_results"

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    principal: Mapped[str] = Column(Text, nullable=False)
    resource_type: Mapped[str] = Column(Text, nullable=False)
    resource: Mapped[str] = Column(Text, nullable=False)
