from database import BaseModel
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import Mapped
from .history_mixin import HistoryMixin
from ..dbos import MetadataDboMixin, IngestionDboMixin


class ResourceHistoryDbo(IngestionDboMixin, MetadataDboMixin, HistoryMixin, BaseModel):
    __tablename__ = "resources_history"

    id: Mapped[int] = Column(Integer)
    fq_name: Mapped[str] = Column(String)
    platform: Mapped[str] = Column(String)
    object_type: Mapped[str] = Column(String)


class ResourceAttributeHistoryDbo(
    IngestionDboMixin, MetadataDboMixin, HistoryMixin, BaseModel
):
    __tablename__ = "resource_attributes_history"

    id: Mapped[int] = Column(Integer)
    fq_name: Mapped[str] = Column(String)
    attribute_key: Mapped[str] = Column(String)
    attribute_value: Mapped[str] = Column(String)
