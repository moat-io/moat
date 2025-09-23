import logging
import sys
from logging import Logger

"""
Functionality:
* Provide a consistent logger
* Allow an entire http transaction to be identified
* Allow an entire ingestion to be identified with its process id
* Provide an event push interface? (maybe th job of something else?

"""

from app_config import AppConfigModelBase


class LoggerConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "logger"
    root_level: str = "INFO"


root_logger_name: str = "moat"
logger_config: LoggerConfig = LoggerConfig().load()

root_logger = logging.getLogger(root_logger_name)  # root logger?
root_logger.setLevel(logger_config.root_level)

# Create a formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s:%(name)s - %(message)s")

# Create a StreamHandler for stdout
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
root_logger.addHandler(stdout_handler)

# Create a StreamHandler for stderr
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setFormatter(formatter)
stderr_handler.setLevel(logging.ERROR)  # Only log ERROR and above to stderr
root_logger.addHandler(stderr_handler)


def get_logger(name: str) -> Logger:
    logger: Logger = logging.getLogger(f"{root_logger_name}.{name}")
    logger.setLevel(AppConfigModelBase.get_value(f"logger.{name}_level", "INFO"))
    return logger
