from database import BaseModel, StringArray
from sqlalchemy import Column, Integer, String, JSON, func, or_, cast, Text, any_, ForeignKey
from sqlalchemy.orm import Mapped, relationship
from . import IngestionDboMixin, MetadataDboMixin


class PrincipalDbo(IngestionDboMixin, MetadataDboMixin, BaseModel):
    __tablename__ = "principals"

    principal_id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String(512))
    first_name: Mapped[str] = Column(String(255))
    last_name: Mapped[str] = Column(String(255))
    user_name: Mapped[str] = Column(String(255))
    email: Mapped[str] = Column(String(255))
    source_type: Mapped[str] = Column(String(100))
    source_uid: Mapped[str] = Column(String(255))
    scim_payload: Mapped[dict] = Column(JSON)
    entitlements: Mapped[list[str]] = Column(StringArray())

    attributes: Mapped[list["PrincipalAttributeDbo"]] = relationship(
        argument="PrincipalAttributeDbo",
        primaryjoin="PrincipalAttributeDbo.fq_name == PrincipalDbo.fq_name",
        foreign_keys=fq_name,
        remote_side="PrincipalAttributeDbo.fq_name",
        uselist=True,
    )

    groups: Mapped[list["PrincipalGroupDbo"]] = relationship(
        argument="PrincipalGroupDbo",
        secondary="principal_group_members",
        primaryjoin="PrincipalDbo.fq_name == PrincipalGroupMemberDbo.member_fq_name",
        secondaryjoin="PrincipalGroupMemberDbo.principal_group_id == PrincipalGroupDbo.principal_group_id",
        uselist=True,
        viewonly=True,
    )


class PrincipalGroupDbo(IngestionDboMixin, MetadataDboMixin, BaseModel):
    __tablename__ = "principal_groups"

    principal_group_id: Mapped[int] = Column(
        Integer, primary_key=True, autoincrement=True
    )
    fq_name: Mapped[str] = Column(String(512))
    source_type: Mapped[str] = Column(String(100))
    source_uid: Mapped[str] = Column(String(255))
    scim_payload: Mapped[dict] = Column(JSON)

    members: Mapped[list["PrincipalGroupMemberDbo"]] = relationship(
        back_populates="principal_group",
        cascade="all, delete-orphan",
    )


class PrincipalGroupMemberDbo(BaseModel):
    __tablename__ = "principal_group_members"

    principal_group_member_id: Mapped[int] = Column(
        Integer, primary_key=True, autoincrement=True
    )
    principal_group_id: Mapped[int] = Column(
        Integer, ForeignKey("principal_groups.principal_group_id"), nullable=False
    )
    member_fq_name: Mapped[str] = Column(String(512), nullable=False)

    principal_group: Mapped["PrincipalGroupDbo"] = relationship(
        back_populates="members"
    )


class PrincipalAttributeDbo(IngestionDboMixin, MetadataDboMixin, BaseModel):
    __tablename__ = "principal_attributes"

    principal_attribute_id: Mapped[int] = Column(
        Integer, primary_key=True, autoincrement=True
    )
    fq_name: Mapped[str] = Column(String(512))
    attribute_key: Mapped[str] = Column(String(255))
    attribute_value: Mapped[str] = Column(String(1024))
