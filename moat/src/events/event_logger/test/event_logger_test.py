from unittest import mock
import events.handlers.src.default_event_log_handler
from ..src.event_logger import EventLogger, EventLoggerConfig
from ...models import EventDto


def test_event_logger():
    # with no config the logger should be null
    with mock.patch.object(
        EventLogger,
        "_load_config",
    ) as mock_load_config:
        mock_load_config.return_value = EventLoggerConfig()
        mock_load_config.return_value.type = None

        event_logger = EventLogger()
        assert not hasattr(event_logger, "_event_queue")

        # write to the queue - should not throw exception or log
        event_logger.log_event(asset="test_event", action="test_action")

    # test we select the correct handler
    with mock.patch.object(
        EventLogger,
        "_load_config",
    ) as mock_load_config:
        mock_load_config.return_value = EventLoggerConfig()
        mock_load_config.return_value.type = "default"

        # write to the queue
        with mock.patch.object(
            events.handlers.src.default_event_log_handler.DefaultEventLogHandler,
            "deliver_event",
        ) as mock_deliver_event:
            event_logger = EventLogger()
            event_logger.log_event(asset="test_event", action="test_action")

            event_logger.terminate()
            event_logger._worker_thread.join()

            mock_deliver_event.assert_called_once_with(
                EventDto(asset="test_event", action="test_action", log="", context={})
            )
