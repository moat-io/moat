from database import BaseModel
from sqlalchemy import Column, DateTime, Integer, String, JSON
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped
from .history_mixin import HistoryMixin
from ..dbos import MetadataDboMixin, IngestionDboMixin


# Mixins must be in this order for the procs to work correctly
class PrincipalHistoryDbo(IngestionDboMixin, MetadataDboMixin, HistoryMixin, BaseModel):
    __tablename__ = "principals_history"

    principal_id: Mapped[int] = Column(Integer)
    fq_name: Mapped[str] = Column(String)
    first_name: Mapped[str] = Column(String)
    last_name: Mapped[str] = Column(String)
    user_name: Mapped[str] = Column(String)
    email: Mapped[str] = Column(String)
    source_type: Mapped[str] = Column(String)
    source_uid: Mapped[str] = Column(String)
    scim_payload: Mapped[dict] = Column(JSON)
    entitlements: Mapped[list[str]] = Column(ARRAY(String()))


class PrincipalAttributeHistoryDbo(
    IngestionDboMixin, MetadataDboMixin, HistoryMixin, BaseModel
):
    __tablename__ = "principal_attributes_history"

    principal_attribute_id: Mapped[int] = Column(Integer)
    fq_name: Mapped[str] = Column(String)
    attribute_key: Mapped[str] = Column(String)
    attribute_value: Mapped[str] = Column(String)


class PrincipalGroupHistoryDbo(
    IngestionDboMixin, MetadataDboMixin, HistoryMixin, BaseModel
):
    __tablename__ = "principal_groups_history"

    principal_group_id: Mapped[int] = Column(Integer)
    fq_name: Mapped[str] = Column(String)
    members: Mapped[list[str]] = Column(ARRAY(String()))
    source_type: Mapped[str] = Column(String)
    source_uid: Mapped[str] = Column(String)
    scim_payload: Mapped[dict] = Column(JSON)
