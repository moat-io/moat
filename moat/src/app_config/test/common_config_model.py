from app_config.src.app_config_model_base import AppConfigModelBase


class TestConfigModel(AppConfigModelBase):
    CONFIG_PREFIX: str = "common"
    db_connection_string: str = None
    super_secret: str = None
    overridden: str = None
    new_in_layer_2: str = None
    boolean_value: bool = False
    int_value: int = 0
