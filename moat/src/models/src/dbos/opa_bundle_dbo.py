from database import BaseModel
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import Mapped
from datetime import datetime
from . import MetadataDboMixin


class OpaBundleDbo(MetadataDboMixin, BaseModel):
    __tablename__ = "opa_bundles"

    opa_bundle_id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = Column(String)
    e_tag: Mapped[str] = Column(String)
    created_at: Mapped[datetime] = Column(DateTime(timezone=True))
    status: Mapped[str] = Column(String)
