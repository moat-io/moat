from app_config import AppConfigModelBase


class HttpEventLoggerConfig(AppConfigModelBase):
    CONFIG_PREFIX = "event_logger.http"

    url: str = None
    headers: str = None
    extra_args: str = ""
    ssl_verify: bool = True
    timeout_ms: int = 500
    flatten_payload: bool = False
