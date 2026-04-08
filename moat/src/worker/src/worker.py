import time
from database import Database
from app_logger import Logger, get_logger
from .worker_config import WorkerConfig
from services.bundle import BundleService
from events import EventLogger

logger: Logger = get_logger("worker")


class Worker:
    def __init__(self):
        self.config: WorkerConfig = WorkerConfig().load()
        self.database: Database = Database()
        self.database.connect()
        self.event_logger: EventLogger = EventLogger()

    def start(self):
        self.event_logger.log_event(asset="worker", action="start")

        logger.info(
            f"Starting worker with interval {self.config.interval_s} seconds and bundle refresh frequency {self.config.interval_s}..."
        )

        while True:
            self._refresh_bundle()

            self._wait()

    def _wait(self):
        logger.info(f"Sleeping for {self.config.interval_s} seconds")
        time.sleep(self.config.interval_s)

        self.event_logger.log_event(asset="worker", action="heartbeat")

    def _refresh_bundle(self) -> None:
        try:
            logger.info("Starting bundle refresh")
            BundleService.refresh_bundle(
                database=self.database, event_logger=self.event_logger
            )

        except Exception as e:
            logger.error(f"Error refreshing bundle: {e}")

    def _clean_up_bundle_storage(self) -> None:
        try:
            with self.database.Session() as session:
                BundleService.clean_up_bundle_storage(
                    session=session, event_logger=self.event_logger
                )
                session.commit()
        except Exception as e:
            logger.error(f"Error cleaning up bundle storage: {e}")
