from unittest import mock

from database import Database
from ingestor.connectors import ConnectorFactory
from ingestor.models import PrincipalAttributeDio, PrincipalDio
from models import (
    ObjectTypeEnum,
    PrincipalDbo,
)
from repositories import IngestionProcessRepository, PrincipalRepository

from ..src.ingestion_controller import IngestionController


class TestConnector:
    NAME = "test"

    def acquire_data(self, platform: str) -> None:
        pass

    def get_principals(self) -> list[PrincipalDio]:
        return [
            PrincipalDio(
                fq_name="bob.hawke",
                first_name="bob",
                last_name="hawke",
                email="bob@bob.net",
                user_name="bobiscool34",
                platform="test",
            ),
            PrincipalDio(
                fq_name="richard.fyneman",
                first_name="richard",
                last_name="fyneman",
                email="richard@bob.net",
                user_name="richardiscool76",
                platform="test",
            ),
        ]

    def get_principal_attributes(self) -> list[PrincipalAttributeDio]:
        return [
            PrincipalAttributeDio(
                fq_name="bob.hawke",
                attribute_key="group",
                attribute_value="bobs",
                platform="test",
            ),
            PrincipalAttributeDio(
                fq_name="bob.hawke",
                attribute_key="group",
                attribute_value="mechanics",
                platform="test",
            ),
            PrincipalAttributeDio(
                fq_name="richard.fyneman",
                attribute_key="group",
                attribute_value="florists",
                platform="test",
            ),
        ]

    def get_data_objects(self) -> list[PrincipalDio]:
        return []

    def get_data_object_attributes(self) -> list[PrincipalAttributeDio]:
        return []


def test_ingest(database_empty: Database):
    ingestion_controller = IngestionController()

    with mock.patch.object(
        ConnectorFactory, "create_by_name", return_value=TestConnector()
    ) as mock_create_by_name:
        # bring in users
        ingestion_controller.ingest(
            connector_name="test",
            object_type=ObjectTypeEnum.PRINCIPAL,
            platform="test",
        )

        # bring in attributes
        ingestion_controller.ingest(
            connector_name="test",
            object_type=ObjectTypeEnum.PRINCIPAL_ATTRIBUTE,
            platform="test",
        )
        assert mock_create_by_name.call_count == 2

    with database_empty.Session.begin() as session:
        # ensure that the process id was created, then updated correctly
        ingestion_process_count, ingestion_processes = (
            IngestionProcessRepository.get_all(session=session)
        )
        assert ingestion_process_count == 2

        assert ingestion_processes[0].object_type == "principal"
        assert ingestion_processes[0].status == "complete"
        assert ingestion_processes[0].started_at <= ingestion_processes[0].completed_at
        assert ingestion_processes[1].object_type == "principal_attribute"

        # test that the right principals and attrs are in the DB transaction tables
        principal_count, principals = PrincipalRepository.get_all(session=session)
        assert principal_count == 2

        bob: PrincipalDbo = [p for p in principals if p.first_name == "bob"][0]
        assert bob.fq_name == "bob.hawke"
        assert bob.last_name == "hawke"
        assert bob.user_name == "bobiscool34"

        assert len(bob.attributes) == 2
        assert [a.attribute_value for a in bob.attributes] == [
            "bobs",
            "mechanics",
        ]

        richard: PrincipalDbo = [p for p in principals if p.first_name == "richard"][0]
        assert richard.fq_name == "richard.fyneman"
        assert richard.last_name == "fyneman"
        assert richard.user_name == "richardiscool76"
        assert len(richard.attributes) == 1

        # ensure the records in the DB have the right process ID
        assert all([p.ingestion_process_id == 1 for p in principals])
        for principal in principals:
            for attribute in principal.attributes:
                assert attribute.ingestion_process_id == 2
