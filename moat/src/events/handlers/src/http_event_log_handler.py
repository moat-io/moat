import requests
from app_logger import Logger, get_logger
from .http_event_log_handler_config import HttpEventLoggerConfig
from .event_log_handler_base import EventLogHandlerBase
from events.models import EventDto

logger: Logger = get_logger("event_log_handler.http")


class HttpEventLogHandler(EventLogHandlerBase):
    NAME = "http"

    def __init__(self):
        self._config = HttpEventLoggerConfig().load()

    def _flatten_dict(self, d: dict, parent_key: str = "", sep: str = "__") -> dict:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key, v))

        return dict(items)

    def deliver_event(self, event: EventDto) -> None:
        # split the extra_args string into a dictionary if they are valid
        extra_args: dict = HttpEventLoggerConfig.split_key_value_pairs(
            self._config.extra_args
        )

        headers: dict = HttpEventLoggerConfig.split_key_value_pairs(
            self._config.headers
        )

        payload = extra_args | event.model_dump()

        if self._config.flatten_payload:
            payload = self._flatten_dict(payload.pop("context")) | payload

        # Send the events to the endpoint
        response = requests.post(
            self._config.url,
            verify=self._config.ssl_verify,
            json=payload,
            headers=headers,
        )

        # Check if the request was successful
        if response.status_code >= 400:
            logger.warning(
                f"Failed to send events to endpoint: {self._config.url} code: {response.status_code} message: {response.text}"
            )
