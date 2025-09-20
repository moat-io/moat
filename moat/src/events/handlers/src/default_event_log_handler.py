import requests
from app_logger import Logger, get_logger
from .http_event_log_handler_config import HttpEventLoggerConfig
from .event_log_handler_base import EventLogHandlerBase
from events.models import EventDto

logger: Logger = get_logger("event_log_handler.default")


class DefaultEventLogHandler(EventLogHandlerBase):
    NAME = "default"

    def deliver_event(self, event: EventDto) -> None:
        logger.info(f"Logging event: {event}")
