from app_logger import Logger, get_logger
from ingestor.connectors import ConnectorBase
from ingestor.models import (
    BaseDio,
    ResourceDio,
)
from models import (
    ResourceStagingDbo,
)
from repositories import (
    ResourceRepository,
)

from .base_ingestion_controller import BaseIngestionController

logger: Logger = get_logger("ingestor.controller.resource")


class ResourceIngestionController(BaseIngestionController):

    def retrieve(self, connector: ConnectorBase) -> list[BaseDio]:
        resource_dios: list[ResourceDio] = connector.get_resources()
        logger.info(f"Retrieved {len(resource_dios)} resources")
        return resource_dios

    def stage(self, session, dios: list[ResourceDio]) -> None:
        resource_stgs: list[ResourceStagingDbo] = []

        for resource_dio in dios:
            do_stg: ResourceStagingDbo = ResourceStagingDbo(
                fq_name=resource_dio.fq_name,
                platform=resource_dio.platform,
                object_type=resource_dio.object_type,
            )
            resource_stgs.append(do_stg)
        session.add_all(resource_stgs)
        logger.info(f"Staged {len(resource_stgs)} resources")

    def merge(
        self, session, ingestion_process_id: int, deactivate_omitted: bool = False
    ) -> None:
        logger.info("Starting merge process for resources")
        ResourceRepository.merge_staging(
            session=session, ingestion_process_id=ingestion_process_id
        )

        logger.info(f"Starting merge deactivate process for resources")
        if deactivate_omitted:
            ResourceRepository.merge_deactivate_staging(
                session=session, ingestion_process_id=ingestion_process_id
            )
