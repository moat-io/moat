from typing import Iterator, Tuple

from app_logger import Logger, get_logger
from database import Database
from models import PrincipalDbo, ResourceDbo
from opa import OpaClient
from repositories import (
    PolicyEvaluatorResultRepository,
    PrincipalRepository,
    ResourceRepository,
)

from .policy_evaluator_config import PolicyEvaluatorConfig

logger: Logger = get_logger("policy_evaluator")

# FilterTables is a table level Trino operation, so only table resources are evaluated
TABLE_OBJECT_TYPE: str = "table"
FILTER_TABLES_ACTION: str = "FilterTables"


class PolicyEvaluator:
    """
    Materialises the result of policy evaluation: for every principal, the list
    of resources they are allowed to access according to OPA.

    It is CLI driven, probably in a k8s cronjob, and is responsible for:
    * listing the principals and the resources for the configured platform
    * batching the resources and asking OPA's batch endpoint which ones each
      principal may access via the FilterTables action
    * merging the allowed (principal, resource) pairs into the
      policy_evaluator_results table, where changed rows get a fresh date and
      revoked rows are soft-deleted
    """

    def __init__(self) -> None:
        self.config: PolicyEvaluatorConfig = PolicyEvaluatorConfig().load()
        self.opa_client: OpaClient = OpaClient()

    @staticmethod
    def _get_database() -> Database:
        database: Database = Database()
        database.connect()
        return database

    @staticmethod
    def _chunk(items: list, batch_size: int) -> Iterator[list]:
        for start in range(0, len(items), batch_size):
            yield items[start : start + batch_size]

    @staticmethod
    def _split_fq_name(fq_name: str) -> Tuple[str, str, str] | None:
        """A table fq_name is 'catalog.schema.table'; anything else is skipped."""
        parts: list[str] = fq_name.split(".")
        if len(parts) != 3:
            return None
        return parts[0], parts[1], parts[2]

    def evaluate(self) -> None:
        logger.info(
            f"Starting policy evaluation for platform '{self.config.platform}' "
            f"with batch size {self.config.batch_size}"
        )
        database: Database = self._get_database()

        with database.Session.begin() as session:
            _, principals = PrincipalRepository.get_all_active(session=session)
            _, resources = ResourceRepository.get_all_by_platform(
                session=session, platform=self.config.platform
            )

            tables: list[ResourceDbo] = [
                resource
                for resource in resources
                if resource.object_type == TABLE_OBJECT_TYPE
                and self._split_fq_name(resource.fq_name) is not None
            ]
            logger.info(
                f"Evaluating {len(principals)} principals against {len(tables)} tables"
            )

            grants: list[Tuple[str, str]] = self._evaluate_grants(
                principals=principals, tables=tables
            )

            counts = PolicyEvaluatorResultRepository.merge_results(
                session=session,
                platform=self.config.platform,
                action=FILTER_TABLES_ACTION,
                grants=grants,
            )
            session.commit()

        logger.info(
            f"Policy evaluation complete: {len(grants)} grants "
            f"(inserted={counts.inserted}, reactivated={counts.reactivated}, "
            f"deactivated={counts.deactivated})"
        )

    def _evaluate_grants(
        self, principals: list[PrincipalDbo], tables: list[ResourceDbo]
    ) -> list[Tuple[str, str]]:
        grants: list[Tuple[str, str]] = []
        for principal in principals:
            for batch in self._chunk(tables, self.config.batch_size):
                allowed_indices: list[int] = self.opa_client.filter_tables_batch(
                    username=principal.user_name,
                    tables=[self._split_fq_name(table.fq_name) for table in batch],
                    path=self.config.opa_batch_path,
                )
                for index in allowed_indices:
                    grants.append((principal.fq_name, batch[index].fq_name))
        return grants
