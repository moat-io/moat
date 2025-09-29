import time
from database import Database
from app_logger import Logger, get_logger
from .worker_config import WorkerConfig
from services.bundle import BundleService

logger: Logger = get_logger("worker")


class Worker:
    def __init__(self):
        self.config: WorkerConfig = WorkerConfig().load()
        self.database: Database = Database()
        self.database.connect()

    def start(self):
        logger.info("Starting worker...")

        while True:
            logger.info("Starting bundle refresh")
            BundleService.refresh_bundle(database=self.database)
            self._wait()

    def _wait(self):
        logger.info(f"Sleeping for {self.config.interval_s} seconds")
        time.sleep(self.config.interval_s)
