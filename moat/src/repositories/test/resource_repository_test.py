from database import Database
from models import (
    ResourceAttributeDbo,
    ResourceDbo,
)

from ..src.resource_repository import ResourceRepository


def test_get_all_by_platform(database: Database) -> None:
    with database.Session.begin() as session:
        # Test with a specific platform
        platform = "trino"  # Using "trino" which is the platform used in test data
        resource_count, resources = ResourceRepository.get_all_by_platform(
            session=session,
            platform=platform,
        )

        # Verify count and type
        assert len(resources) == resource_count == 14
        assert all([isinstance(r, ResourceDbo) for r in resources])

        # Verify all resources have the requested platform
        assert all([r.platform == platform for r in resources])

        # spot check resources
        shippers_resource = next(
            r for r in resources if r.fq_name == "datalake.logistics.shippers"
        )
        assert shippers_resource.object_type == "table"
        assert len(shippers_resource.attributes) == 2
        assert isinstance(shippers_resource.attributes[0], ResourceAttributeDbo)
        assert shippers_resource.attributes[0].attribute_key == "Subject"
        assert shippers_resource.attributes[0].attribute_value == "Sales"
        assert shippers_resource.attributes[1].attribute_key == "AccessLevel"
        assert shippers_resource.attributes[1].attribute_value == "Commercial"

        # Verify customer_markets.type_name resource
        type_name_resource = next(
            r
            for r in resources
            if r.fq_name == "datalake.sales.customer_markets.type_name"
        )
        assert type_name_resource.object_type == "column"
        # assert type_name_resource.mask == "substring(type_name,1,3)" # TODO
        assert len(type_name_resource.attributes) == 2
        assert isinstance(type_name_resource.attributes[0], ResourceAttributeDbo)
        assert type_name_resource.attributes[0].attribute_key == "Subject"
        assert type_name_resource.attributes[0].attribute_value == "IT"
        assert type_name_resource.attributes[1].attribute_key == "AccessLevel"
        assert type_name_resource.attributes[1].attribute_value == "Restricted"
