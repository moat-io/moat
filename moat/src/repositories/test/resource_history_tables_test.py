from database import Database
from models import (
    ResourceDbo,
    ResourceAttributeDbo,
    ResourceHistoryDbo,
    ResourceAttributeHistoryDbo,
)


def test_resource_history_table(database_empty: Database) -> None:
     # Create a resource
    with database_empty.Session.begin() as session:
        # Create a resource
        resource = ResourceDbo()
        resource.fq_name = "test.resource"
        resource.platform = "test_platform"
        resource.object_type = "test_object"
        session.add(resource)

        session.flush()
        resource_id = resource.id
        session.commit()

    # Verify the history tables have the insert records
    with database_empty.Session.begin() as session:
        # Check resources_history
        resources_history = (
            session.query(ResourceHistoryDbo)
            .filter(ResourceHistoryDbo.id == resource_id)
            .all()
        )

        assert len(resources_history) == 1
        assert resources_history[0].history_change_operation == "I"  # Insert operation
        assert resources_history[0].fq_name == "test.resource"
        assert resources_history[0].platform == "test_platform"
        assert resources_history[0].object_type == "test_object"

    # Update the resource
    with database_empty.Session.begin() as session:
        # Update the resource
        resource = session.query(ResourceDbo).filter_by(id=resource_id).one()
        resource.platform = "updated_platform"
        resource.object_type = "updated_object"
        session.commit()

    # Verify the history tables have the update records
    with database_empty.Session.begin() as session:
        # Check resources_history
        resources_history = (
            session.query(ResourceHistoryDbo)
            .filter(ResourceHistoryDbo.id == resource_id)
            .all()
        )

        assert len(resources_history) == 2
        assert resources_history[1].history_change_operation == "U"  # Update operation
        assert resources_history[1].fq_name == "test.resource"
        assert resources_history[1].platform == "updated_platform"
        assert resources_history[1].object_type == "updated_object"

    # Delete the resource and resource attribute
    with database_empty.Session.begin() as session:
        resource = session.query(ResourceDbo).filter_by(id=resource_id).one()
        session.delete(resource)
        session.commit()

    # Verify the history tables have the delete records
    with database_empty.Session.begin() as session:
        # Check resources_history
        resources_history = (
            session.query(ResourceHistoryDbo)
            .filter(ResourceHistoryDbo.id == resource_id)
            .all()
        )

        assert len(resources_history) == 3
        assert resources_history[2].history_change_operation == "D"  # Delete operation
        assert resources_history[2].fq_name == "test.resource"


def test_resource_attribute_history_table(database_empty: Database) -> None:
    # Create a resource attribute
    with database_empty.Session.begin() as session:
        # Create a resource attribute
        attribute = ResourceAttributeDbo()
        attribute.fq_name = "test.resource"  # Same fq_name to link to the resource
        attribute.attribute_key = "test_key"
        attribute.attribute_value = "test_value"
        session.add(attribute)

        session.flush()
        attribute_id = attribute.id
        session.commit()

    # Verify the history tables have the insert records
    with database_empty.Session.begin() as session:
        # Check resource_attributes_history
        attributes_history = (
            session.query(ResourceAttributeHistoryDbo)
            .filter(ResourceAttributeHistoryDbo.id == attribute_id)
            .all()
        )

        assert len(attributes_history) == 1
        assert attributes_history[0].history_change_operation == "I"  # Insert operation
        assert attributes_history[0].fq_name == "test.resource"
        assert attributes_history[0].attribute_key == "test_key"
        assert attributes_history[0].attribute_value == "test_value"

    # Update the resource and resource attribute
    with database_empty.Session.begin() as session:
        # Update the resource attribute
        attribute = session.query(ResourceAttributeDbo).filter_by(id=attribute_id).one()
        attribute.attribute_key = "updated_key"
        attribute.attribute_value = "updated_value"

        # Commit to trigger the history table updates
        session.commit()

    # Verify the history tables have the update records
    with database_empty.Session.begin() as session:
        # Check resource_attributes_history
        attributes_history = (
            session.query(ResourceAttributeHistoryDbo)
            .filter(ResourceAttributeHistoryDbo.id == attribute_id)
            .all()
        )

        assert len(attributes_history) == 2
        assert attributes_history[1].history_change_operation == "U"  # Update operation
        assert attributes_history[1].fq_name == "test.resource"
        assert attributes_history[1].attribute_key == "updated_key"
        assert attributes_history[1].attribute_value == "updated_value"

    # Delete the resource and resource attribute
    with database_empty.Session.begin() as session:
        # Delete the resource attribute first (to avoid foreign key constraints)
        attribute = session.query(ResourceAttributeDbo).filter_by(id=attribute_id).one()
        session.delete(attribute)
        session.commit()

    # Verify the history tables have the delete records
    with database_empty.Session.begin() as session:
        # Check resource_attributes_history
        attributes_history = (
            session.query(ResourceAttributeHistoryDbo)
            .filter(ResourceAttributeHistoryDbo.id == attribute_id)
            .all()
        )

        assert len(attributes_history) == 3
        assert attributes_history[2].history_change_operation == "D"  # Delete operation
        assert attributes_history[2].fq_name == "test.resource"
