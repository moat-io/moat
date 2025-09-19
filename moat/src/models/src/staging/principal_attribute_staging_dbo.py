from database import BaseModel
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Mapped


class PrincipalAttributeStagingDbo(BaseModel):
    __tablename__ = "principal_attributes_stg"

    MERGE_KEYS: list[str] = ["fq_name"]
    UPDATE_COLS: list[str] = ["attribute_key", "attribute_value"]

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String)
    attribute_key: Mapped[str] = Column(String)
    attribute_value: Mapped[str] = Column(String)
