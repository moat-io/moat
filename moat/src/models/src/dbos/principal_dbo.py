from database import BaseModel
from sqlalchemy import Column, DateTime, Integer, String, JSON
from sqlalchemy.orm import Mapped, relationship
from . import IngestionDboMixin, MetadataDboMixin
from sqlalchemy.dialects.postgresql import ARRAY


class PrincipalDbo(IngestionDboMixin, MetadataDboMixin, BaseModel):
    __tablename__ = "principals"

    principal_id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String)
    first_name: Mapped[str] = Column(String)
    last_name: Mapped[str] = Column(String)
    user_name: Mapped[str] = Column(String)
    email: Mapped[str] = Column(String)
    source_type: Mapped[str] = Column(String)
    source_uid: Mapped[str] = Column(String)
    scim_payload: Mapped[dict] = Column(JSON)
    entitlements: Mapped[list[str]] = Column(ARRAY(String()))

    attributes: Mapped[list["PrincipalAttributeDbo"]] = relationship(
        argument="PrincipalAttributeDbo",
        primaryjoin="PrincipalAttributeDbo.fq_name == PrincipalDbo.fq_name",
        foreign_keys=fq_name,
        remote_side="PrincipalAttributeDbo.fq_name",
        uselist=True,
    )

    groups: Mapped[list["PrincipalGroupDbo"]] = relationship(
        argument="PrincipalGroupDbo",
        primaryjoin="PrincipalGroupDbo.members.any(PrincipalDbo.fq_name)",
        foreign_keys=fq_name,
        remote_side="PrincipalGroupDbo.members",
        uselist=True,
    )


class PrincipalGroupDbo(IngestionDboMixin, MetadataDboMixin, BaseModel):
    __tablename__ = "principal_groups"

    principal_group_id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String())
    members: Mapped[list[str]] = Column(ARRAY(String()))
    source_type: Mapped[str] = Column(String)
    source_uid: Mapped[str] = Column(String)
    scim_payload: Mapped[dict] = Column(JSON)


class PrincipalAttributeDbo(IngestionDboMixin, MetadataDboMixin, BaseModel):
    __tablename__ = "principal_attributes"

    principal_attribute_id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String)
    attribute_key: Mapped[str] = Column(String)
    attribute_value: Mapped[str] = Column(String)
