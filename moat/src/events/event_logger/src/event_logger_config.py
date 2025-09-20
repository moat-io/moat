from app_config import AppConfigModelBase


class EventLoggerConfig(AppConfigModelBase):
    CONFIG_PREFIX = "event_logger"

    type: str | None = None
