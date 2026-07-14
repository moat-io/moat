import time
from database import Database
from app_logger import Logger, get_logger
from .worker_config import WorkerConfig
from services.bundle import BundleService
from events import EventLogger
from policy_evaluator.src.policy_evaluator import PolicyEvaluator

logger: Logger = get_logger("worker")


class Worker:
    def __init__(self):
        self.config: WorkerConfig = WorkerConfig().load()
        self.database: Database = Database()
        self.database.connect()
        self.event_logger: EventLogger = EventLogger()
        self.policy_evaluator: PolicyEvaluator = PolicyEvaluator()

    def start(self):
        self.event_logger.log_event(asset="worker", action="start")

        logger.info(
            f"Starting worker with interval {self.config.interval_s} seconds and bundle refresh frequency {self.config.interval_s}..."
        )

        while True:
            updated_bundle_platforms: list[str] = self._refresh_bundle()
            self._clean_up_bundle_storage()

            # if updated_bundle_platforms:
            self._assure_opa_bundles()
            self._evaluate_policies()  # TODO link the recreated policy bundle to the evaluator so we are not out of sync

            self._wait()

    def _wait(self):
        logger.info(f"Sleeping for {self.config.interval_s} seconds")
        time.sleep(self.config.interval_s)

        self.event_logger.log_event(asset="worker", action="heartbeat")

    def _refresh_bundle(self) -> list[str]:
        try:
            logger.info("Starting bundle refresh")
            updated_bundle_platforms: list[str] = BundleService.refresh_bundle(
                database=self.database, event_logger=self.event_logger
            )
            return updated_bundle_platforms

        except Exception as e:
            logger.error(f"Error refreshing bundle: {e}")
            return []

    def _clean_up_bundle_storage(self) -> None:
        try:
            with self.database.Session() as session:
                BundleService.clean_up_bundle_storage(
                    session=session, event_logger=self.event_logger
                )
                session.commit()
        except Exception as e:
            logger.error(f"Error cleaning up bundle storage: {e}")

    def _assure_opa_bundles(self) -> None:
        logger.info(f"Assurance of OPA bundle not currently supported")

    def _evaluate_policies(self) -> None:
        try:
            logger.info("Starting policy evaluation")
            with self.database.Session() as session:
                self.policy_evaluator.evaluate(session=session)
        except Exception as e:
            logger.error(f"Error evaluating policies: {e}")
