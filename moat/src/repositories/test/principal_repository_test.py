from database import Database
from models import (
    PrincipalAttributeStagingDbo,
    PrincipalDbo,
    PrincipalStagingDbo,
    PrincipalAttributeDbo,
)

from ..src.principal_repository import PrincipalRepository


def test_truncate_staging(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        session.add(PrincipalStagingDbo())
        session.add(PrincipalAttributeStagingDbo())
        session.commit()

    with database.Session.begin() as session:
        assert session.query(PrincipalStagingDbo).count() == 1
        assert session.query(PrincipalAttributeStagingDbo).count() == 1

        repo.truncate_staging_tables(session=session)
        session.commit()

    with database.Session.begin() as session:
        assert session.query(PrincipalStagingDbo).count() == 0
        assert session.query(PrincipalAttributeStagingDbo).count() == 0


def test_get_all(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        principal_count, principals = repo.get_all(
            session=session,
        )

        assert principal_count == 5
        assert all([isinstance(p, PrincipalDbo) for p in principals])
        assert len(principals) == 5

        # entitlements
        assert all([len(p.entitlements) >= 1 for p in principals])

        # attributes
        assert all([len(p.attributes) >= 1 for p in principals])


def test_get_by_username(database: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    with database.Session.begin() as session:
        principal: PrincipalDbo = repo.get_by_username(
            session=session, user_name="alice"
        )

        assert principal.user_name == "alice"
        assert principal.first_name == "Alice"
        assert principal.last_name == "Cooper"

        assert principal.entitlements == [
            "sales_data_analysis",
            "marketing_strategy",
            "it_team_supervision",
            "code_repository_access",
        ]

        assert {
            attr.attribute_key: attr.attribute_value for attr in principal.attributes
        } == {
            "Commercial": "Marketing,IT,Sales",
            "Employee": "True",
            "Privacy": "Marketing",
            "Redact": "PII",
            "Restricted": "Marketing,IT",
        }
