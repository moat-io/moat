from app_config.src.app_config_model_base import AppConfigModelBase


class TestConfigModel(AppConfigModelBase):
    CONFIG_PREFIX: str = "common"
    db_connection_string: str | None = None
    super_secret: str | None = None
    overridden: str | None = None
    new_in_layer_2: str | None = None
    boolean_value: bool = False
    int_value: int = 0
    str_int_value: int = 0
