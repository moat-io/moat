import json
import os
import uuid
from datetime import datetime

from database import Database
from models import (
    IngestionProcessDbo,
    PrincipalAttributeDbo,
    PrincipalGroupDbo,
    PrincipalDbo,
    ResourceDbo,
    ResourceAttributeDbo,
)

from .database_config import DatabaseConfig


class DatabaseSeeder:
    def __init__(self, db: Database):
        self.db = db

    @staticmethod
    def _get_principals() -> list[PrincipalDbo]:
        with open(
            os.path.join(DatabaseConfig.load().seed_data_path, "principals.json")
        ) as json_file:
            mock_users: list[dict] = json.load(json_file)
            principals: list[PrincipalDbo] = []

            for mock_user in mock_users:
                principal_dbo: PrincipalDbo = PrincipalDbo()
                principal_dbo.fq_name = mock_user.get("username")
                principal_dbo.activated_at = datetime.now()
                principal_dbo.deactivated_at = None
                principal_dbo.first_name = mock_user.get("first_name")
                principal_dbo.last_name = mock_user.get("last_name")
                principal_dbo.user_name = mock_user.get("username")
                principal_dbo.email = mock_user.get("email")
                principal_dbo.source_type = "seeder"
                principal_dbo.source_uid = mock_user.get("username")
                principal_dbo.entitlements = mock_user.get("entitlements")

                # apply attributes to principal
                for k, v in mock_user.get("attributes").items():
                    if isinstance(v, list):
                        v = ",".join(v)

                    principal_attribute_dbo = PrincipalAttributeDbo()
                    principal_attribute_dbo.fq_name = principal_dbo.fq_name
                    principal_attribute_dbo.attribute_key = k
                    principal_attribute_dbo.attribute_value = v
                    principal_attribute_dbo.activated_at = datetime.utcnow()
                    principal_dbo.attributes.append(principal_attribute_dbo)

                principals.append(principal_dbo)
            return principals

    @staticmethod
    def _get_groups() -> list[PrincipalGroupDbo]:
        with open(
            os.path.join(DatabaseConfig.load().seed_data_path, "principals.json")
        ) as json_file:
            mock_users: list[dict] = json.load(json_file)
            groups: list[PrincipalGroupDbo] = []

            unique_groups = set(
                [
                    g
                    for sublist in [u.get("groups", []) for u in mock_users]
                    for g in sublist
                ]
            )

            for group in unique_groups:
                principal_group_dbo = PrincipalGroupDbo()
                principal_group_dbo.fq_name = group
                principal_group_dbo.source_type = "seeder"
                principal_group_dbo.source_uid = group
                principal_group_dbo.members = []

                for mock_user in mock_users:
                    if group in mock_user.get("groups", []):
                        principal_group_dbo.members.append(mock_user.get("username"))

                groups.append(principal_group_dbo)
            return groups

    @staticmethod
    def _get_resources() -> list[ResourceDbo]:
        with open(
            os.path.join(DatabaseConfig.load().seed_data_path, "resources.json")
        ) as json_file:
            resources_data: list[dict] = json.load(json_file)
            resources: list[ResourceDbo] = []

            for resource_data in resources_data:
                resource_dbo: ResourceDbo = ResourceDbo()
                resource_dbo.fq_name = resource_data.get("fq_name")
                resource_dbo.platform = resource_data.get("platform")
                resource_dbo.object_type = resource_data.get("object_type")

                # If there are attributes in the resource data, add them
                if "attributes" in resource_data:
                    for attr in resource_data.get("attributes", []):
                        resource_attr_dbo = ResourceAttributeDbo()
                        resource_attr_dbo.fq_name = resource_dbo.fq_name
                        resource_attr_dbo.attribute_key = attr.get("key")
                        resource_attr_dbo.attribute_value = attr.get("value")
                        resource_dbo.attributes.append(resource_attr_dbo)

                resources.append(resource_dbo)

            return resources

    def _get_process_id(self, object_type: str) -> int:
        # create the process id
        ingestion_process_dbo: IngestionProcessDbo = IngestionProcessDbo()
        with self.db.Session.begin() as session:
            session.add(ingestion_process_dbo)
            ingestion_process_dbo.source = "Seed"
            ingestion_process_dbo.object_type = object_type
            session.flush()
            ingestion_process_id = ingestion_process_dbo.ingestion_process_id
            session.commit()
        return ingestion_process_id

    def _ingest_objects(self, object_type: str, object_list: list):
        ingestion_process_id = self._get_process_id(object_type)

        for obj in object_list:
            obj.ingestion_process_id = ingestion_process_id

        with self.db.Session.begin() as session:
            session.add_all(object_list)
            ingestion_process_dbo = session.get(
                IngestionProcessDbo, ingestion_process_id
            )
            ingestion_process_dbo.completed_at = datetime.utcnow()
            ingestion_process_dbo.status = "completed"
            session.commit()

    def seed(self):
        self._ingest_objects("Principal", self._get_principals())
        self._ingest_objects("Resources", self._get_resources())
        self._ingest_objects("Groups", self._get_groups())
