from unittest import mock
from events.models import EventDto
from ..src.http_event_log_handler import HttpEventLogHandler


def test_log_event():
    handler = HttpEventLogHandler()

    # Create test event - action in context should be ignored
    event = EventDto(
        asset="test_asset",
        action="test_action",
        log="Test log message",
        context={
            "key1": "value1",
            "key2": "value2",
            "actions": {
                "sub_key1": "sub_value",
                "sub_key2": {"sub_key3": "sub_value3"},
            },
        },
    )

    # Setup mock response
    with mock.patch("requests.post") as mock_post:
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        # Call the method under test
        handler.deliver_event(event)

        # Verify that requests.post was called with the correct arguments
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args

    # Check URL
    assert args[0] == "https://localhost:9000/event"

    # Check SSL verification
    assert kwargs["verify"] is True

    # Check headers
    assert kwargs["headers"] == {
        "content-type": "application/json",
        "Api-Key": "API_KEY",
    }

    # Check payload
    assert kwargs["json"] == {
        "eventType": "MoatEvent",
        "asset": "test_asset",
        "action": "test_action",
        "log": "Test log message",
        "key1": "value1",
        "key2": "value2",
        "actions__sub_key1": "sub_value",
        "actions__sub_key2__sub_key3": "sub_value3",
    }
