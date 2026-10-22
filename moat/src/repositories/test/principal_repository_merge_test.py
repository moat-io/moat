from database import Database
from models import (
    PrincipalDbo,
    PrincipalStagingDbo,
)

from ..src.principal_repository import PrincipalRepository


def test_merge_principals_staging(database_empty: Database) -> None:
    repo: PrincipalRepository = PrincipalRepository()

    # scenario 1: empty target tables
    with database_empty.Session.begin() as session:
        # populate staging table
        ps1: PrincipalStagingDbo = PrincipalStagingDbo()
        ps1.fq_name = "abigail.fleming"
        ps1.first_name = "Abigail"
        ps1.last_name = "Fleming"
        ps1.user_name = "abigail.fleming"
        ps1.email = "abigail.fleming@mail.com"
        session.add(ps1)

        ps2: PrincipalStagingDbo = PrincipalStagingDbo()
        ps2.fq_name = "boris.johnstone"
        ps2.first_name = "Boris"
        ps2.last_name = "Johnstone"
        ps2.user_name = "boris.johnstone"
        ps2.email = "boris.johnstone@mail.com"
        session.add(ps2)
        session.commit()

    # merge it
    with database_empty.Session.begin() as session:
        insert_row_count, update_row_count = repo.merge_staging(
            session=session, ingestion_process_id=1
        )
        assert insert_row_count == 2
        assert update_row_count == 0

        row_count: int = repo.merge_deactivate_staging(
            session=session, ingestion_process_id=2
        )
        assert row_count == 0
        session.commit()

    # test it
    with database_empty.Session.begin() as session:
        count, principals = repo.get_all(session=session)
        assert count == 2
        assert principals[0].principal_id == 1
        assert principals[0].fq_name == "abigail.fleming"
        assert principals[0].ingestion_process_id == 1
        assert principals[0].active
        assert principals[1].principal_id == 2
        assert principals[1].fq_name == "boris.johnstone"
        assert principals[1].ingestion_process_id == 1
        assert principals[1].active

    # scenario 2: append to target tables
    with database_empty.Session.begin() as session:
        # populate staging table
        ps3: PrincipalStagingDbo = PrincipalStagingDbo()
        ps3.fq_name = "frank.herbert"
        ps3.first_name = "Frank"
        ps3.last_name = "Herbert"
        ps3.user_name = "frank.herbert"
        ps3.email = "frank.herbert@mail.com"
        session.add(ps3)
        session.commit()

    # merge it
    with database_empty.Session.begin() as session:
        insert_row_count, update_row_count = repo.merge_staging(
            session=session, ingestion_process_id=2
        )
        assert insert_row_count == 1
        assert update_row_count == 0

        row_count: int = repo.merge_deactivate_staging(
            session=session, ingestion_process_id=2
        )
        assert row_count == 0
        session.commit()

    # test it
    with database_empty.Session.begin() as session:
        count, principals = repo.get_all(session=session)
        # one record should be changed
        assert 1 == len([p for p in principals if p.ingestion_process_id == 2])
        assert count == 3

    # scenario 3: update target tables - includes two deletes
    with database_empty.Session.begin() as session:
        principal: PrincipalDbo = repo.get_by_id(session=session, principal_id=1)
        assert principal.first_name == "Abigail"

        # truncate staging table
        repo.truncate_principal_staging_table(session=session)

        # populate staging table
        ps: PrincipalStagingDbo = PrincipalStagingDbo()
        ps.fq_name = "abigail.fleming"
        ps.first_name = "Anne"
        ps.last_name = "Hathaway"  # name changed
        ps.user_name = "abigail.fleming"
        ps.email = "abigail.fleming@mail.com"
        session.add(ps)
        session.commit()

    # merge it
    with database_empty.Session.begin() as session:
        insert_row_count, update_row_count = repo.merge_staging(
            session=session, ingestion_process_id=3
        )
        deactivate_row_count: int = repo.merge_deactivate_staging(
            session=session, ingestion_process_id=3
        )
        assert insert_row_count == 0
        assert update_row_count == 1
        assert deactivate_row_count == 2  # two deletes
        session.commit()

    # test it
    with database_empty.Session.begin() as session:
        principal: PrincipalDbo = repo.get_by_id(session=session, principal_id=1)
        assert principal.first_name == "Anne"
        assert principal.last_name == "Hathaway"

        count, principals = repo.get_all(session=session)
        # only the record present in staging should remain
        assert 1 == count
        assert principals[0].first_name == "Anne"
        assert principals[0].ingestion_process_id == 3
        assert principals[0].active

    # scenario 4: a principal deactivated outside of ingestion - by SCIM for
    # example - is reactivated when the source still reports it, even though
    # none of the other columns have changed
    with database_empty.Session.begin() as session:
        principal: PrincipalDbo = repo.get_by_id(session=session, principal_id=1)
        principal.active = False
        session.commit()

    # merge it - the staging table still holds the same single record
    with database_empty.Session.begin() as session:
        insert_row_count, update_row_count = repo.merge_staging(
            session=session, ingestion_process_id=4
        )
        assert insert_row_count == 0
        assert update_row_count == 1
        session.commit()

    # test it
    with database_empty.Session.begin() as session:
        principal: PrincipalDbo = repo.get_by_id(session=session, principal_id=1)
        assert principal.active
        assert principal.ingestion_process_id == 4
