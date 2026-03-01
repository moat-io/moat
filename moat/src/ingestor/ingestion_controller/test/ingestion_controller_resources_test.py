from unittest import mock

from database import Database
from ingestor.connectors import ConnectorFactory
from ingestor.models import ResourceAttributeDio, ResourceDio
from models import (
    ObjectTypeEnum,
    ResourceDbo,
)
from repositories import IngestionProcessRepository, ResourceRepository

from ..src.ingestion_controller import IngestionController


class TestConnector:
    NAME = "test"

    def acquire_data(self, platform: str) -> None:
        pass

    # These methods are kept for compatibility but not used in this test
    def get_principals(self) -> list[ResourceDio]:
        return []

    def get_principal_attributes(self) -> list[ResourceAttributeDio]:
        return []

    def get_resources(self) -> list[ResourceDio]:
        return [
            ResourceDio(
                fq_name="customers",
                object_type="table",
                platform="test",
            ),
            ResourceDio(
                fq_name="orders",
                object_type="table",
                platform="test",
            ),
        ]

    def get_resource_attributes(self) -> list[ResourceAttributeDio]:
        return [
            ResourceAttributeDio(
                fq_name="customers",
                attribute_key="PII",
                attribute_value="true",
                platform="test",
            ),
            ResourceAttributeDio(
                fq_name="customers",
                attribute_key="PII",
                attribute_value="false",
                platform="test",
            ),
            ResourceAttributeDio(
                fq_name="orders",
                attribute_key="PII",
                attribute_value="false",
                platform="test",
            ),
        ]


def test_ingest(database_empty: Database):
    ingestion_controller = IngestionController()

    with mock.patch.object(
        ConnectorFactory, "create_by_name", return_value=TestConnector()
    ) as mock_create_by_name:
        # bring in users
        ingestion_controller.ingest(
            connector_name="test",
            object_type=ObjectTypeEnum.RESOURCE,
            platform="test",
        )

        # bring in attributes
        ingestion_controller.ingest(
            connector_name="test",
            object_type=ObjectTypeEnum.RESOURCE_ATTRIBUTE,
            platform="test",
        )
        assert mock_create_by_name.call_count == 2

    with database_empty.Session.begin() as session:
        # ensure that the process id was created, then updated correctly
        ingestion_process_count, ingestion_processes = (
            IngestionProcessRepository.get_all(session=session)
        )
        assert ingestion_process_count == 2

        assert ingestion_processes[0].object_type == "resource"
        assert ingestion_processes[0].status == "complete"
        assert ingestion_processes[0].started_at <= ingestion_processes[0].completed_at
        assert ingestion_processes[1].object_type == "resource_attribute"

        # test that the right resources and attrs are in the DB transaction tables
        resource_count, resources = ResourceRepository.get_all(session=session)
        assert resource_count == 2

        customers: ResourceDbo = [r for r in resources if r.fq_name == "customers"][0]
        assert customers.fq_name == "customers"
        assert customers.object_type == "table"

        assert len(customers.attributes) == 2
        assert sorted([a.attribute_value for a in customers.attributes]) == [
            "false",
            "true",
        ]

        orders: ResourceDbo = [r for r in resources if r.fq_name == "orders"][0]
        assert orders.fq_name == "orders"
        assert orders.object_type == "table"
        assert len(orders.attributes) == 1

        # ensure the records in the DB have the right process ID
        assert all([r.ingestion_process_id == 1 for r in resources])
        for resource in resources:
            for attribute in resource.attributes:
                assert attribute.ingestion_process_id == 2
