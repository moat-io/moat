from database import Database
from models import (
    ResourceDbo,
    ResourceStagingDbo,
    ResourceAttributeStagingDbo,
    ResourceAttributeDbo,
)

from ..src.resource_repository import ResourceRepository


def test_merge_with_null_source(database_empty: Database) -> None:
    """
    BUG: it is possible to merge a record with a null fq_name into the target table.
    """
    repo: ResourceRepository = ResourceRepository()

    # seed the table
    with database_empty.Session.begin() as session:
        # populate staging table
        rs1: ResourceStagingDbo = ResourceStagingDbo()
        rs1.fq_name = "resource1"
        rs1.platform = "trino"
        session.add(rs1)

        rs2: ResourceStagingDbo = ResourceStagingDbo()
        rs2.fq_name = "resource2"
        rs2.platform = "trino"
        session.add(rs2)

        rs3: ResourceStagingDbo = ResourceStagingDbo()
        session.add(rs3)

        rs4: ResourceStagingDbo = ResourceStagingDbo()
        session.add(rs4)
        session.commit()

    # merge
    with database_empty.Session.begin() as session:
        row_count: int = repo.merge_staging(session=session, ingestion_process_id=1)
        assert row_count == 2
        session.commit()

    # test
    with database_empty.Session.begin() as session:
        # Query the resources table to verify the merge
        resources = session.query(ResourceDbo).all()
        assert len(resources) == 2
        assert [r.fq_name for r in resources] == ["resource1", "resource2"]
