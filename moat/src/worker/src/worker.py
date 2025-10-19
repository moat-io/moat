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
            try:
                logger.info("Starting bundle refresh")
                BundleService.refresh_bundle(database=self.database)

            except Exception as e:
                logger.error(f"Error refreshing bundle: {e}")

            finally:
                self._wait()

    def _wait(self):
        logger.info(f"Sleeping for {self.config.interval_s} seconds")
        time.sleep(self.config.interval_s)

        self.event_logger.log_event(asset="worker", action="heartbeat")
