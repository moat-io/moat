from database import BaseModel
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import Mapped
from sqlalchemy.sql import expression

from . import MetadataDboMixin


class PolicyEvaluatorResultDbo(MetadataDboMixin, BaseModel):
    __tablename__ = "policy_evaluator_results"

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    principal_fq_name: Mapped[str] = Column(String(512))
    resource_fq_name: Mapped[str] = Column(String(512))
    platform: Mapped[str] = Column(String(100))
    action: Mapped[str] = Column(String(100))
    active: Mapped[bool] = Column(
        Boolean, default=True, server_default=expression.true()
    )
