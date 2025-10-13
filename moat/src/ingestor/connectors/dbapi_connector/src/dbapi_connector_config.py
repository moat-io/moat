from app_config import AppConfigModelBase


class DBAPIConnectorConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "dbapi_connector"
    client_type: str = None
    data_object_table_column_query: str = None
    fq_name_key: str = "fq_name"
    object_type_key: str = "object_type"
    attribute_key_key: str = "attribute_key"
    attribute_value_key: str = "attribute_value"
