from database import BaseModel
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Mapped


class ResourceStagingDbo(BaseModel):
    __tablename__ = "resources_stg"

    MERGE_KEYS: list[str] = ["fq_name"]
    UPDATE_COLS: list[str] = ["platform", "object_type"]

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String)
    platform: Mapped[str] = Column(String)
    object_type: Mapped[str] = Column(String)


class ResourceAttributeStagingDbo(BaseModel):
    __tablename__ = "resource_attributes_stg"

    MERGE_KEYS: list[str] = ["fq_name", "attribute_key"]
    UPDATE_COLS: list[str] = ["attribute_value"]

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String)
    attribute_key: Mapped[str] = Column(String)
    attribute_value: Mapped[str] = Column(String)
