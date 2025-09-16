from database import BaseModel
from sqlalchemy import Column, DateTime, Integer, String, JSON
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.sql.functions import current_timestamp
from sqlalchemy.dialects.postgresql import ARRAY
from .common_mixin_dbo import IngestionDboMixin


class PrincipalDbo(IngestionDboMixin, BaseModel):
    __tablename__ = "principals"

    principal_id: int = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: str = Column(String)
    first_name: str = Column(String)
    last_name: str = Column(String)
    user_name: str = Column(String)
    email: str = Column(String)
    source_type: str = Column(String)
    source_uid: str = Column(String)
    scim_payload: dict = Column(JSON)
    entitlements: list[str] = Column(ARRAY(String()))

    record_updated_date: str = Column(
        DateTime(timezone=True), server_default=current_timestamp()
    )
    record_updated_by: str = Column(String)

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


class PrincipalGroupDbo(IngestionDboMixin, BaseModel):
    __tablename__ = "principal_groups"

    principal_group_id = Column(Integer, primary_key=True, autoincrement=True)
    fq_name = Column(String())
    members = Column(ARRAY(String()))
    source_type: str = Column(String)
    source_uid: str = Column(String)
    scim_payload: dict = Column(JSON)


class PrincipalAttributeDbo(IngestionDboMixin, BaseModel):
    __tablename__ = "principal_attributes"

    principal_attribute_id: int = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: str = Column(String)
    attribute_key: str = Column(String)
    attribute_value: str = Column(String)
