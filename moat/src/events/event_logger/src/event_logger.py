from app_logger import Logger, get_logger
import queue
import threading
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

        # Start the worker thread
        self._event_queue = queue.Queue()
        self._stop_event = threading.Event()
        self._worker_thread = threading.Thread(
            target=self._process_events,
            args=(self._event_queue, self._stop_event, self._config),
            daemon=True,
        )
        self._worker_thread.start()

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

    def log_event(
        self, asset: str, action: str, log: str = "", context: dict = None
    ) -> None:
        event = EventDto(asset=asset, action=action, log=log, context=context or {})
        if self._config.type is None:
            logger.debug(f"No event handler configured, ignoring event")
        else:
            # Log an event by adding it to the queue.
            self._event_queue.put(event)
            logger.info(f"Event queued: {event}")

    def _process_events(
        self,
        _queue: queue.Queue,
        stop_event: threading.Event,
        config: EventLoggerConfig,
    ) -> None:
        """
        Process events from the queue and send them to the HTTP endpoint.
        This method runs in a separate thread.
        """
        event_handler = self._create_event_handler(config)
        while not stop_event.is_set():
            try:
                event = _queue.get(timeout=1.0)
            except queue.Empty:
                continue

            try:
                event_handler.deliver_event(event)
            except Exception as e:
                logger.error(f"Error processing event: {e}")

            _queue.task_done()

    def terminate(self) -> None:
        logger.info("Terminating event logger")
        self._stop_event.set()
