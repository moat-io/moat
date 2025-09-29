from datetime import datetime
from sqlalchemy.sql import text, func
from models import (
    PrincipalGroupDbo,
    PrincipalGroupHistoryDbo,
)

from .repository_base import RepositoryBase


class PrincipalGroupRepository(RepositoryBase):

    @staticmethod
    def get_group_by_name(session, fq_name: str) -> PrincipalGroupDbo:
        return (
            session.query(PrincipalGroupDbo)
            .filter(PrincipalGroupDbo.fq_name == fq_name)
            .first()
        )

    @staticmethod
    def get_latest_change_timestamp(session) -> datetime:
        return RepositoryBase.get_latest_timestamp_for_model_history(
            session=session, model=PrincipalGroupHistoryDbo
        )
