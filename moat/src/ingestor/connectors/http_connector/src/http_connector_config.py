from app_config import AppConfigModelBase
from collections import namedtuple
from enum import Enum
from pydantic import Field


AttributeMapping = namedtuple("AttributeMapping", ["jsonpath", "regex"])


class PaginationKeyLocation(Enum):
    RESPONSE_HEADER = "response_header"
    RESPONSE_BODY = "response_body"


class HttpConnectorConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "http_connector"
    auth_method: str = "api-key"
    content_pattern: str = "$[*]"  # "can be also items[*] or $[*]"
    api_key: str | None = None

    url: str | None = None
    username: str | None = None
    password: str | None = None
    ssl_verify: bool = True
    certificate_path: str | None = None
    oauth2_endpoint: str | None = None
    oauth2_client_id: str | None = None
    oauth2_client_secret: str | None = None
    oauth2_grant_type: str | None = None
    oauth2_scope: str | None = None
    pagination_enabled: bool = False
    pagination_size: int = 250
    pagination_target_key: str = None
    pagination_key_location: str = None
    pagination_key_name: str = None
    pagination_offset_key: str = None

    @staticmethod
    def attribute_jsonpath_mapping(
        prefix: str = "", attributes_to_map: list[str] = None
    ) -> dict:
        attribute_mapping: dict[str, AttributeMapping] = {}
        prefix = f"{prefix}_" if prefix else ""
        for attribute in attributes_to_map:
            attribute_mapping[attribute] = AttributeMapping(
                jsonpath=HttpConnectorConfig.get_value(
                    f"{HttpConnectorConfig.CONFIG_PREFIX}.{prefix}{attribute}_jsonpath"
                ),
                regex=HttpConnectorConfig.get_value(
                    f"{HttpConnectorConfig.CONFIG_PREFIX}.{prefix}{attribute}_regex"
                ),
            )
        return attribute_mapping
