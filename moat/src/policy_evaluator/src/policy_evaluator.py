import json
import tarfile
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator, Tuple

from app_logger import Logger, get_logger
from database import Database
from models import OpaBundleDbo, ResourceDbo
from opa import OpaClient
from repositories import (
    PolicyEvaluatorResultRepository,
    PrincipalRepository,
    ResourceRepository,
)
from services.bundle import BundleService

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
    * reading the principals from the current OPA bundle for the platform, so
      the evaluation sees exactly what OPA sees; the tables and the
      user_name -> fq_name mapping come from the DB
    * grouping principals into cohorts that share the same bundle profile
      (attributes, entitlements, groups); OPA sees identical data for every
      member, so one representative is evaluated and its result fanned out.
      Configured special users are always evaluated on their own
    * batching the resources and asking OPA's batch endpoint which ones each
      cohort may access via the FilterTables action, with up to max_workers
      requests in flight
    * merging the allowed (principal, resource) pairs into the
      policy_evaluator_results table, where changed rows get a fresh date and
      revoked rows are soft-deleted; changed rows are stamped with the bundle
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
            opa_bundle, principals = self._load_bundle_principals(session=session)
            fq_names: dict[str, str] = PrincipalRepository.get_fq_names_by_user_name(
                session=session
            )
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
                f"Evaluating {len(principals)} principals from bundle "
                f"'{opa_bundle.e_tag}' against {len(tables)} tables"
            )

            grants: list[Tuple[str, str]] = self._evaluate_grants(
                principals=principals, fq_names=fq_names, tables=tables
            )

            counts = PolicyEvaluatorResultRepository.merge_results(
                session=session,
                platform=self.config.platform,
                action=FILTER_TABLES_ACTION,
                grants=grants,
                opa_bundle=opa_bundle,
            )
            session.commit()

        logger.info(
            f"Policy evaluation complete: {len(grants)} grants "
            f"(inserted={counts.inserted}, reactivated={counts.reactivated}, "
            f"deactivated={counts.deactivated})"
        )

    def _load_bundle_principals(self, session) -> Tuple[OpaBundleDbo, dict]:
        """The current bundle and its principals, keyed by user_name."""
        opa_bundle: OpaBundleDbo | None = BundleService.get_current_bundle_metadata(
            session=session, platform=self.config.platform
        )
        if opa_bundle is None:
            raise ValueError(f"No bundle found for platform '{self.config.platform}'")

        with tarfile.open(opa_bundle.bundle_path) as tar:
            # opa build merges all data files into a single root data.json
            member = next(m for m in tar if m.name.lstrip("/") == "data.json")
            data: dict = json.load(tar.extractfile(member))
        return opa_bundle, data[self.config.platform]["principals"]

    @staticmethod
    def _profile_key(principal: dict) -> str:
        """Everything the bundle holds about a principal, so equal keys get equal decisions."""
        return json.dumps(
            {key: sorted(value or []) for key, value in principal.items()},
            sort_keys=True,
        )

    def _group_principals(
        self, principals: dict, fq_names: dict[str, str]
    ) -> list[list[Tuple[str, str]]]:
        """Cohorts of (user_name, fq_name) that share a bundle profile."""
        special_users: set[str] = {
            user.strip() for user in self.config.special_users.split(",") if user.strip()
        }
        cohorts: dict[str, list[Tuple[str, str]]] = {}
        for user_name, principal in principals.items():
            if user_name not in fq_names:
                logger.warning(f"Skipping bundle principal '{user_name}' missing from the DB")
                continue
            key: str = (
                f"special:{user_name}"
                if user_name in special_users
                else self._profile_key(principal)
            )
            cohorts.setdefault(key, []).append((user_name, fq_names[user_name]))
        return list(cohorts.values())

    def _evaluate_grants(
        self, principals: dict, fq_names: dict[str, str], tables: list[ResourceDbo]
    ) -> list[Tuple[str, str]]:
        cohorts: list[list[Tuple[str, str]]] = self._group_principals(
            principals=principals, fq_names=fq_names
        )
        logger.info(
            f"Grouped {len(principals)} principals into {len(cohorts)} cohorts, "
            f"evaluating with {self.config.max_workers} workers"
        )

        batches: list[Tuple[list[ResourceDbo], list]] = [
            (batch, [self._split_fq_name(table.fq_name) for table in batch])
            for batch in self._chunk(tables, self.config.batch_size)
        ]
        jobs: list[Tuple[list[Tuple[str, str]], list[ResourceDbo], list]] = [
            (cohort, batch, split_tables)
            for cohort in cohorts
            for batch, split_tables in batches
        ]

        def run(job) -> list[int]:
            cohort, _, split_tables = job
            return self.opa_client.filter_tables_batch(
                username=cohort[0][0],
                tables=split_tables,
                path=self.config.opa_batch_path,
            )

        grants: list[Tuple[str, str]] = []
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            for (cohort, batch, _), allowed_indices in zip(
                jobs, executor.map(run, jobs)
            ):
                for index in allowed_indices:
                    for _, fq_name in cohort:
                        grants.append((fq_name, batch[index].fq_name))
        return grants
