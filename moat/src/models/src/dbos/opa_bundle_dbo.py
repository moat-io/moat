import os
from database import BaseModel
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Mapped
from . import MetadataDboMixin


class OpaBundleDbo(MetadataDboMixin, BaseModel):
    __tablename__ = "opa_bundles"

    @property
    def bundle_path(self):
        return os.path.join(self.bundle_directory, self.bundle_filename)

    opa_bundle_id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = Column(String(100))
    e_tag: Mapped[str] = Column(String(255))
    bundle_filename: Mapped[str] = Column(String(512))
    bundle_directory: Mapped[str] = Column(String(1024))
    policy_hash: Mapped[str] = Column(String(255))
