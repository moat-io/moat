from database import BaseModel
from sqlalchemy import Boolean, Column, Index, Integer, String
from sqlalchemy.orm import Mapped
from sqlalchemy.sql import expression

from . import MetadataDboMixin


class PolicyEvaluatorResultDbo(MetadataDboMixin, BaseModel):
    __tablename__ = "policy_evaluator_results"
    __table_args__ = (
        Index(
            "ix_policy_evaluator_results_principal",
            "platform",
            "action",
            "principal_fq_name",
            "resource_fq_name",
            mysql_length={"principal_fq_name": 255, "resource_fq_name": 255},
        ),
        Index(
            "ix_policy_evaluator_results_resource",
            "platform",
            "action",
            "resource_fq_name",
            "principal_fq_name",
            mysql_length={"principal_fq_name": 255, "resource_fq_name": 255},
        ),
    )

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    principal_fq_name: Mapped[str] = Column(String(512))
    resource_fq_name: Mapped[str] = Column(String(512))
    platform: Mapped[str] = Column(String(100))
    action: Mapped[str] = Column(String(100))
    # the bundle that produced the row's last change
    bundle_e_tag: Mapped[str] = Column(String(255))
    bundle_policy_hash: Mapped[str] = Column(String(255))
    active: Mapped[bool] = Column(
        Boolean, default=True, server_default=expression.true()
    )
