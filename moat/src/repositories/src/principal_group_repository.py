from models import (
    PrincipalGroupDbo,
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
