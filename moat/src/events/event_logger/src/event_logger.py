from app_logger import Logger, get_logger
from ...models import EventDto
from events.handlers.src.event_log_handler_base import EventLogHandlerBase
from .event_logger_config import EventLoggerConfig

logger: Logger = get_logger("event_logger")


class EventLogger:
    def __init__(self):
        self._config: EventLoggerConfig = self._load_config()

        # if not configured, exit now
        if self._config.type is None:
            logger.info(f"No event handler configured")
            return

        logger.info(f"Creating event logger with type: {self._config.type}")
        self._event_handler: EventLogHandlerBase = EventLogger._create_event_handler(
            config=self._config
        )

    @staticmethod
    def _create_event_handler(config: EventLoggerConfig) -> EventLogHandlerBase:
        # load the subclasses of EventLogHandlerBase
        event_handler_classes: list[type[EventLogHandlerBase]] = (
            EventLogHandlerBase.__subclasses__()
        )

        # select the handler class
        event_handler: type[EventLogHandlerBase] = next(
            (e for e in event_handler_classes if e.NAME == config.type), None
        )
        return event_handler()

    @staticmethod
    def _load_config() -> EventLoggerConfig:
        return EventLoggerConfig().load()

    def log_events(
        self, asset: str, action: str, log: str = "", contexts: list[dict] = None
    ):
        if self._config.type is None:
            logger.info(f"No event handler configured, ignoring event")
            return

        try:
            events: list[EventDto] = [
                EventDto(asset=asset, action=action, log=log, context=context)
                for context in contexts or []
            ]
            self._event_handler.deliver_events(events=events)

        except Exception as e:
            logger.error(f"Error processing event: {e}")
            raise

    def log_event(
        self, asset: str, action: str, log: str = "", context: dict = None
    ) -> None:
        self.log_events(
            asset=asset,
            action=action,
            log=log,
            contexts=[context] if context else [{}],
        )
