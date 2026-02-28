from database import BaseModel
from . import IngestionDboMixin, MetadataDboMixin
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Mapped, relationship


class ResourceDbo(IngestionDboMixin, MetadataDboMixin, BaseModel):
    __tablename__ = "resources"

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String(512))
    platform: Mapped[str] = Column(String(100))
    object_type: Mapped[str] = Column(String(100))

    attributes: Mapped[list["ResourceAttributeDbo"]] = relationship(
        "ResourceAttributeDbo",
        primaryjoin="ResourceAttributeDbo.fq_name == ResourceDbo.fq_name",
        foreign_keys=fq_name,
        remote_side="ResourceAttributeDbo.fq_name",
        uselist=True,
    )


class ResourceAttributeDbo(IngestionDboMixin, MetadataDboMixin, BaseModel):
    __tablename__ = "resource_attributes"

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String(512))
    attribute_key: Mapped[str] = Column(String(255))
    attribute_value: Mapped[str] = Column(String(2048))
