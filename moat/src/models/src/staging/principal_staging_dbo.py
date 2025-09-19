from database import BaseModel
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Mapped


class PrincipalStagingDbo(BaseModel):
    __tablename__ = "principals_staging"

    MERGE_KEYS: list[str] = ["fq_name"]
    UPDATE_COLS: list[str] = ["first_name", "last_name", "user_name", "email"]

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    fq_name: Mapped[str] = Column(String)
    first_name: Mapped[str] = Column(String)
    last_name: Mapped[str] = Column(String)
    user_name: Mapped[str] = Column(String)
    email: Mapped[str] = Column(String)
