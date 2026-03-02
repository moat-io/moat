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

        # send a single event
        with mock.patch.object(
            events.handlers.src.default_event_log_handler.DefaultEventLogHandler,
            "deliver_events",
        ) as mock_deliver_events:
            event_logger = EventLogger()
            event_logger.log_event(asset="test_event", action="test_action")

            mock_deliver_events.assert_called_once_with(
                events=[
                    EventDto(
                        asset="test_event", action="test_action", log="", context={}
                    )
                ]
            )

        # send multiple events
        with mock.patch.object(
            events.handlers.src.default_event_log_handler.DefaultEventLogHandler,
            "deliver_events",
        ) as mock_deliver_events:
            event_logger = EventLogger()
            event_logger.log_events(
                asset="test_event",
                action="test_action",
                contexts=[{"k1": "v1"}, {"k2": "v2"}],
            )

            mock_deliver_events.assert_called_once()
            assert mock_deliver_events.call_args.kwargs["events"] == [
                EventDto(
                    asset="test_event",
                    action="test_action",
                    log="",
                    context={"k1": "v1"},
                ),
                EventDto(
                    asset="test_event",
                    action="test_action",
                    log="",
                    context={"k2": "v2"},
                ),
            ]
