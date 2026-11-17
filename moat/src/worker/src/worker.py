import time
from database import Database
from app_logger import Logger, get_logger
from .worker_config import WorkerConfig
from services.bundle import BundleService
from events import EventLogger
from policy_evaluator import PolicyEvaluator
from policy_evaluator.src.policy_evaluator_config import PolicyEvaluatorConfig

logger: Logger = get_logger("worker")


class Worker:
    def __init__(self):
        self.config: WorkerConfig = WorkerConfig().load()
        self.database: Database = Database()
        self.database.connect()
        self.event_logger: EventLogger = EventLogger()
        self.policy_evaluator_config: PolicyEvaluatorConfig = (
            PolicyEvaluatorConfig().load()
        )

    def start(self):
        self.event_logger.log_event(asset="worker", action="start")

        logger.info(
            f"Starting worker with interval {self.config.interval_s} seconds and bundle refresh frequency {self.config.interval_s}..."
        )

        while True:
            self._refresh_bundle()
            self._clean_up_bundle_storage()
            self._wait()

    def _wait(self):
        logger.info(f"Sleeping for {self.config.interval_s} seconds")
        time.sleep(self.config.interval_s)

        self.event_logger.log_event(asset="worker", action="heartbeat")

    def _refresh_bundle(self) -> None:
        try:
            logger.info("Starting bundle refresh")
            previous_e_tag: str | None = self._get_bundle_e_tag()
            BundleService.refresh_bundle(
                database=self.database, event_logger=self.event_logger
            )

        except Exception as e:
            logger.error(f"Error refreshing bundle: {e}")
            return

        if (
            self.policy_evaluator_config.evaluate_on_bundle_change
            and self._get_bundle_e_tag() != previous_e_tag
        ):
            self._evaluate_policy()

    def _get_bundle_e_tag(self) -> str | None:
        """ETag of the current bundle for the evaluator's platform; a new bundle means its data or policy changed."""
        with self.database.Session() as session:
            opa_bundle = BundleService.get_current_bundle_metadata(
                session=session, platform=self.policy_evaluator_config.platform
            )
            return opa_bundle.e_tag if opa_bundle else None

    def _evaluate_policy(self) -> None:
        try:
            logger.info("New bundle generated, starting policy evaluation")
            PolicyEvaluator().evaluate()
        except Exception as e:
            logger.error(f"Error evaluating policy: {e}")

    def _clean_up_bundle_storage(self) -> None:
        try:
            with self.database.Session() as session:
                BundleService.clean_up_bundle_storage(
                    session=session, event_logger=self.event_logger
                )
                session.commit()
        except Exception as e:
            logger.error(f"Error cleaning up bundle storage: {e}")
