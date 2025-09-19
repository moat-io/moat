from dataclasses import dataclass

from app_logger import Logger, get_logger
from database import Database
from ingestor.connectors import ConnectorBase, ConnectorFactory
from ingestor.models import (
    BaseDio,
)
from models import (
    ObjectTypeEnum,
)
from repositories import (
    IngestionProcessRepository,
    PrincipalRepository,
    ResourceRepository,
)
from .base_ingestion_controller import BaseIngestionController

from .principal_ingestion_controller import PrincipalIngestionController
from .principal_attribute_ingestion_controller import (
    PrincipalAttributeIngestionController,
)
from .resource_ingestion_controller import ResourceIngestionController
from .resource_attribute_ingestion_controller import (
    ResourceAttributeIngestionController,
)

logger: Logger = get_logger("ingestor.controller")


@dataclass
class IngestionStats:
    principal_count: int = 0
    principal_attribute_count: int = 0
    resource_count: int = 0
    resource_attribute_count: int = 0


class IngestionController:
    """
    Ingestion controller executes specific ingestion operations from different sources
    It is called by the CLI, probably in a k8s cronjob
    It is responsible for:
    * setting the status of ingestion_process objects
    * maintaining the staging tables
    * creating the ingestion connector classes (e.g ldap / trino)
    * pulling full or partial datasets from source systems into staging tables for:
      * principals
      * principal attributes
      * attribute groups / group attributes
      * data objects (tables & columns)
    * storing the results of ingestion operations, counts, errors etc
    * merging the results with the main tables

    * the ingestion controller will get the DBOs from the relevant connectors, i.e:
      * principals and attributes from LDAP
      * attribute groups from a JSON/YAML file
      * data objects (tables & columns) with attributes from trino

    * this controller should also provide a mutex via the ingestion process table to lock the staging tables
    """

    @staticmethod
    def _get_database() -> Database:
        database: Database = Database()
        database.connect()
        return database

    @staticmethod
    def _get_controller(
        object_type: ObjectTypeEnum,
    ) -> BaseIngestionController:
        if object_type == ObjectTypeEnum.PRINCIPAL:
            return PrincipalIngestionController()
        if object_type == ObjectTypeEnum.PRINCIPAL_ATTRIBUTE:
            return PrincipalAttributeIngestionController()
        if object_type == ObjectTypeEnum.RESOURCE:
            return ResourceIngestionController()
        if object_type == ObjectTypeEnum.RESOURCE_ATTRIBUTE:
            return ResourceAttributeIngestionController()
        raise ValueError(
            f"Object type {object_type} could not be mapped to an ingestion controller"
        )

    def ingest(
        self,
        connector_name: str,
        object_type: ObjectTypeEnum,
        platform: str,
        deactivate_omitted: bool = True,
    ) -> None:
        logger.info("Starting ingestion process")

        # TODO create mutex - in ingestion process table?
        database: Database = self._get_database()

        # create connector
        connector: ConnectorBase = ConnectorFactory.create_by_name(
            connector_name=connector_name
        )

        # create controller
        controller = self._get_controller(object_type=object_type)

        # load data into connector
        connector.acquire_data(platform=platform)

        # create the process id & truncate staging
        process_id: int = self._initialise_ingestion_process(
            database=database, connector_name=connector_name, object_types=[object_type]
        )

        # ingest into staging
        with database.Session.begin() as session:
            try:
                logger.info(f"Starting retrieve")
                dios: list[BaseDio] = controller.retrieve(
                    connector=connector,
                )

                # stage
                logger.info(f"Starting stage")
                controller.stage(session, dios)
                session.commit()
                logger.info(f"Commited data into staging table")
            except Exception as e:
                logger.error(f"Ingestion failed with error: {str(e)}")
                raise
                # TODO return error code

        # merge into main tables
        with database.Session.begin() as session:
            try:
                # merge
                logger.info(f"Starting merge")
                controller.merge(
                    session=session,
                    ingestion_process_id=process_id,
                    deactivate_omitted=deactivate_omitted,
                )

            finally:
                # close ingestion process
                logger.info(
                    f"Completing ingestion process with process ID: {process_id}"
                )
                IngestionProcessRepository.complete_process(
                    session=session, ingestion_process_id=process_id
                )
                session.commit()

    @staticmethod
    def _initialise_ingestion_process(
        database: Database, connector_name: str, object_types: list[ObjectTypeEnum]
    ) -> int:
        with database.Session.begin() as session:
            if ObjectTypeEnum.PRINCIPAL in object_types:
                PrincipalRepository.truncate_principal_staging_table(session=session)
            if ObjectTypeEnum.PRINCIPAL_ATTRIBUTE in object_types:
                PrincipalRepository.truncate_principal_attribute_staging_table(
                    session=session
                )
            if ObjectTypeEnum.RESOURCE in object_types:
                ResourceRepository.truncate_resource_staging_table(session=session)
            if ObjectTypeEnum.RESOURCE_ATTRIBUTE in object_types:
                ResourceRepository.truncate_resource_attribute_staging_table(
                    session=session
                )

            process_id: int = IngestionProcessRepository.create(
                session=session,
                source=connector_name,
                object_types=object_types,
            )
            session.commit()

        logger.info(
            f"Created ingestion process with id: {process_id} for object types: {object_types}"
        )
        return process_id
