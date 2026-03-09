from app_config import AppConfigModelBase


class BundleGeneratorConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "bundle_generator"
    temp_directory: str = None
    static_rego_file_path: str = "opa"
