from app_config import AppConfigModelBase


class WorkerConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "worker"
    interval_s: int = 60
