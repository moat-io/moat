from app_config import AppConfigModelBase


class BundlerConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "bundler"
    bundle_directory: str = "/tmp/bundles"
    bundle_retention_days: int = 7
