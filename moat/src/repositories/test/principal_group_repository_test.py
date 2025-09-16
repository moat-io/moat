from database import Database
from models import PrincipalGroupDbo
from ..src.principal_group_repository import PrincipalGroupRepository


def test_get_principal_groups(database: Database) -> None:
    repo: PrincipalGroupRepository = PrincipalGroupRepository()

    with database.Session.begin() as session:
        group: PrincipalGroupDbo = repo.get_group_by_name(
            session=session, fq_name="IT_ANALYSTS_GL"
        )
        assert group.fq_name == "IT_ANALYSTS_GL"
        assert group.members == ["bob"]

        group: PrincipalGroupDbo = repo.get_group_by_name(
            session=session, fq_name="SALES_ANALYSTS_GL"
        )
        assert group.fq_name == "SALES_ANALYSTS_GL"
        assert group.members == ["alice"]
