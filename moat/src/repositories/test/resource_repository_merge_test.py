from database import Database
from models import (
    ResourceDbo,
    ResourceStagingDbo,
    ResourceAttributeStagingDbo,
    ResourceAttributeDbo,
)

from ..src.resource_repository import ResourceRepository


def test_merge_resources_staging(database_empty: Database) -> None:
    repo: ResourceRepository = ResourceRepository()

    # scenario 1: empty target tables
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
        session.commit()

    # merge it
    with database_empty.Session.begin() as session:
        row_count: int = repo.merge_staging(session=session, ingestion_process_id=1)
        assert row_count == 2
        session.commit()

    # test it
    with database_empty.Session.begin() as session:
        # Query the resources table to verify the merge
        resources = session.query(ResourceDbo).all()
        assert len(resources) == 2
        assert resources[0].id == 1
        assert resources[0].fq_name == "resource1"
        assert resources[0].platform == "trino"
        assert resources[0].ingestion_process_id == 1
        assert resources[0].active
        assert resources[1].id == 2
        assert resources[1].fq_name == "resource2"
        assert resources[1].platform == "trino"
        assert resources[1].ingestion_process_id == 1
        assert resources[1].active

    # scenario 2: append to target tables
    with database_empty.Session.begin() as session:
        # populate staging table
        rs3: ResourceStagingDbo = ResourceStagingDbo()
        rs3.fq_name = "resource3"
        rs3.fq_name = "redshift"
        session.add(rs3)
        session.commit()

    # merge it
    with database_empty.Session.begin() as session:
        row_count: int = repo.merge_staging(session=session, ingestion_process_id=2)
        assert row_count == 1
        session.commit()

    # test it
    with database_empty.Session.begin() as session:
        # Query the resources table to verify the merge
        count, resources = repo.get_all(session=session)
        # one record should be changed
        assert 1 == len([r for r in resources if r.ingestion_process_id == 2])
        assert count == 3

    # scenario 3: update target tables
    with database_empty.Session.begin() as session:
        resource: ResourceDbo = repo.get_by_id(session=session, resource_id=1)
        assert resource.fq_name == "resource1"

        # truncate staging table
        repo.truncate_resource_staging_table(session=session)

        # populate staging table
        rs: ResourceStagingDbo = ResourceStagingDbo()
        rs.fq_name = "resource1"
        rs.platform = "postgres"
        session.add(rs)
        session.commit()

    # merge it
    with database_empty.Session.begin() as session:
        row_count: int = repo.merge_staging(session=session, ingestion_process_id=3)
        assert row_count == 1
        session.commit()

    # test it
    with database_empty.Session.begin() as session:
        # Query the resources table to verify the merge
        resource = repo.get_by_id(session=session, resource_id=1)
        assert resource.fq_name == "resource1"
        assert resource.platform == "postgres"
        assert resource.ingestion_process_id == 3
        assert resource.active

        # Verify all resources
        count, resources = repo.get_all(session=session)
        assert count == 3
        assert len([r for r in resources if r.ingestion_process_id == 1]) == 1
        assert len([r for r in resources if r.ingestion_process_id == 2]) == 1
        assert len([r for r in resources if r.ingestion_process_id == 3]) == 1


def test_merge_resource_attributes_staging(database_empty: Database) -> None:
    repo: ResourceRepository = ResourceRepository()

    # scenario 1: empty target tables
    with database_empty.Session.begin() as session:
        # populate staging table
        rs1: ResourceAttributeStagingDbo = ResourceAttributeStagingDbo()
        rs1.fq_name = "resource1"
        rs1.attribute_key = "key1"
        rs1.attribute_value = "value1"
        session.add(rs1)

        rs2: ResourceAttributeStagingDbo = ResourceAttributeStagingDbo()
        rs2.fq_name = "resource1"
        rs2.attribute_key = "key2"
        rs2.attribute_value = "value2"
        session.add(rs2)
        session.commit()

    # merge it
    with database_empty.Session.begin() as session:
        row_count: int = repo.merge_attributes_staging(
            session=session, ingestion_process_id=1
        )
        assert row_count == 2
        session.commit()

    # test it
    with database_empty.Session.begin() as session:
        # Query the resource attributes table to verify the merge
        attributes = (
            session.query(ResourceAttributeDbo).order_by(ResourceAttributeDbo.id).all()
        )
        assert len(attributes) == 2
        assert attributes[0].id == 1
        assert attributes[0].fq_name == "resource1"
        assert attributes[0].attribute_key == "key1"
        assert attributes[0].attribute_value == "value1"
        assert attributes[0].ingestion_process_id == 1
        assert attributes[0].active
        assert attributes[1].id == 2
        assert attributes[1].fq_name == "resource1"
        assert attributes[1].attribute_key == "key2"
        assert attributes[1].attribute_value == "value2"
        assert attributes[1].ingestion_process_id == 1
        assert attributes[1].active

    # scenario 2: append to target tables
    with database_empty.Session.begin() as session:
        # truncate staging table
        repo.truncate_resource_attribute_staging_table(session=session)

        # populate staging table
        rs3: ResourceAttributeStagingDbo = ResourceAttributeStagingDbo()
        rs3.fq_name = "resource1"
        rs3.attribute_key = "key3"
        rs3.attribute_value = "value3"
        session.add(rs3)
        session.commit()

    # merge it
    with database_empty.Session.begin() as session:
        row_count: int = repo.merge_attributes_staging(
            session=session, ingestion_process_id=2
        )
        assert row_count == 1
        session.commit()

    # test it
    with database_empty.Session.begin() as session:
        # Query the resource attributes table to verify the merge
        attributes = session.query(ResourceAttributeDbo).all()
        # one record should be changed
        assert 1 == len([a for a in attributes if a.ingestion_process_id == 2])
        assert len(attributes) == 3

    # scenario 3: update target tables
    with database_empty.Session.begin() as session:
        attribute = (
            session.query(ResourceAttributeDbo)
            .filter(ResourceAttributeDbo.id == 1)
            .one_or_none()
        )
        assert attribute.attribute_value == "value1"

        # truncate staging table
        repo.truncate_resource_attribute_staging_table(session=session)

        # populate staging table
        rs: ResourceAttributeStagingDbo = ResourceAttributeStagingDbo()
        rs.fq_name = "resource1"
        rs.attribute_key = "key1"
        rs.attribute_value = "updated_value1"
        session.add(rs)
        session.commit()

    # merge it
    with database_empty.Session.begin() as session:
        row_count: int = repo.merge_attributes_staging(
            session=session, ingestion_process_id=3
        )
        assert row_count == 1
        session.commit()

    # test it
    with database_empty.Session.begin() as session:
        # Query the resource attributes table to verify the merge
        attribute = (
            session.query(ResourceAttributeDbo)
            .filter(ResourceAttributeDbo.id == 1)
            .one_or_none()
        )
        assert attribute.fq_name == "resource1"
        assert attribute.attribute_key == "key1"
        assert attribute.attribute_value == "updated_value1"
        assert attribute.ingestion_process_id == 3
        assert attribute.active

        # Verify all attributes
        attributes = session.query(ResourceAttributeDbo).all()
        assert len(attributes) == 3
        assert len([a for a in attributes if a.ingestion_process_id == 1]) == 1
        assert len([a for a in attributes if a.ingestion_process_id == 2]) == 1
        assert len([a for a in attributes if a.ingestion_process_id == 3]) == 1
