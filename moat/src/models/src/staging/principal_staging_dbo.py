from database import BaseModel
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Mapped


class PrincipalStagingDbo(BaseModel):
    __tablename__ = "principals_staging"

    MERGE_KEYS: list[str] = ["fq_name"]
    UPDATE_COLS: list[str] = ["first_name", "last_name", "user_name", "email"]

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String(512))
    first_name: Mapped[str] = Column(String(255))
    last_name: Mapped[str] = Column(String(255))
    user_name: Mapped[str] = Column(String(255))
    email: Mapped[str] = Column(String(255))
