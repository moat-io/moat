from database import BaseModel, StringArray
from sqlalchemy import Column, Integer, String, JSON
from sqlalchemy.orm import Mapped
from .history_mixin import HistoryMixin
from ..dbos import MetadataDboMixin, IngestionDboMixin


# Mixins must be in this order for the procs to work correctly
class PrincipalHistoryDbo(IngestionDboMixin, MetadataDboMixin, HistoryMixin, BaseModel):
    __tablename__ = "principals_history"

    principal_id: Mapped[int] = Column(Integer)
    fq_name: Mapped[str] = Column(String(512))
    first_name: Mapped[str] = Column(String(255))
    last_name: Mapped[str] = Column(String(255))
    user_name: Mapped[str] = Column(String(255))
    email: Mapped[str] = Column(String(255))
    source_type: Mapped[str] = Column(String(100))
    source_uid: Mapped[str] = Column(String(255))
    scim_payload: Mapped[dict] = Column(JSON)
    entitlements: Mapped[list[str]] = Column(StringArray())


class PrincipalGroupHistoryDbo(
    IngestionDboMixin, MetadataDboMixin, HistoryMixin, BaseModel
):
    __tablename__ = "principal_groups_history"

    principal_group_id: Mapped[int] = Column(Integer)
    fq_name: Mapped[str] = Column(String(512))
    members: Mapped[list[str]] = Column(StringArray())
    source_type: Mapped[str] = Column(String(100))
    source_uid: Mapped[str] = Column(String(255))
    scim_payload: Mapped[dict] = Column(JSON)


class PrincipalAttributeHistoryDbo(
    IngestionDboMixin, MetadataDboMixin, HistoryMixin, BaseModel
):
    __tablename__ = "principal_attributes_history"

    principal_attribute_id: Mapped[int] = Column(Integer)
    fq_name: Mapped[str] = Column(String(512))
    attribute_key: Mapped[str] = Column(String(255))
    attribute_value: Mapped[str] = Column(String(1024))
