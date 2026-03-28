from collections import namedtuple
from urllib.parse import parse_qsl

from app_config import AppConfigModelBase

AttributeMapping = namedtuple("AttributeMapping", ["jsonpath", "regex"])


class OmConnectorConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "om_connector"

    base_url: str | None = None
    auth_token: str | None = None
    auth_header_name: str = "Authorization"
    auth_header_prefix: str = "Bearer"
    ssl_verify: bool = True
    certificate_path: str | None = None
    timeout_s: int = 30
    page_size: int = 100

    principal_endpoint: str = "/api/v1/users"
    principal_query_params: str = "limit=100"
    principal_content_pattern: str = "$.data[*]"
    principal_after_jsonpath: str = "$.paging.after"

    principal_attribute_endpoint: str | None = None
    principal_attribute_query_params: str = "limit=100"
    principal_attribute_content_pattern: str = "$.data[*]"
    principal_attribute_after_jsonpath: str = "$.paging.after"

    resource_endpoint: str = "/api/v1/tables"
    resource_endpoints: str | None = None
    resource_query_params: str = "limit=100"
    resource_content_pattern: str = "$.data[*]"
    resource_after_jsonpath: str = "$.paging.after"
    resource_object_type_default: str | None = None
    resource_type_mapping: str = (
        "table=table,column=column,regular=table,external=table,view=table,"
        "materializedview=table,materialized_view=table,iceberg=table"
    )

    resource_attribute_endpoint: str | None = None
    resource_attribute_query_params: str = "limit=100"
    resource_attribute_content_pattern: str = "$.data[*]"
    resource_attribute_after_jsonpath: str = "$.paging.after"

    @staticmethod
    def parse_query_params(query_params: str | None) -> dict[str, str]:
        if not query_params:
            return {}

        if "&" not in query_params and "," in query_params:
            parts: list[str] = query_params.split(",")
            if all("=" in p for p in parts):
                parsed = {}
                for part in parts:
                    key, value = part.split("=", 1)
                    if key:
                        parsed[key] = value
                return parsed

        return dict(parse_qsl(query_params, keep_blank_values=True))

    @staticmethod
    def parse_csv(value: str | None) -> list[str]:
        if not value:
            return []
        return [part.strip() for part in value.split(",") if part.strip()]

    @staticmethod
    def parse_resource_type_mapping(mapping: str | None) -> dict[str, str]:
        key_values = AppConfigModelBase.split_key_value_pairs(mapping or "")
        return {
            str(key).strip().lower(): str(value).strip().lower()
            for key, value in key_values.items()
            if str(key).strip()
        }

    @staticmethod
    def attribute_jsonpath_mapping(
        prefix: str = "", attributes_to_map: list[str] | None = None
    ) -> dict[str, AttributeMapping]:
        attributes_to_map = attributes_to_map or []
        attribute_mapping: dict[str, AttributeMapping] = {}
        prefix = f"{prefix}_" if prefix else ""
        for attribute in attributes_to_map:
            attribute_mapping[attribute] = AttributeMapping(
                jsonpath=OmConnectorConfig.get_value(
                    f"{OmConnectorConfig.CONFIG_PREFIX}.{prefix}{attribute}_jsonpath"
                ),
                regex=OmConnectorConfig.get_value(
                    f"{OmConnectorConfig.CONFIG_PREFIX}.{prefix}{attribute}_regex"
                ),
            )
        return attribute_mapping
