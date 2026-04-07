import json
import re
from dataclasses import dataclass, fields, is_dataclass
from re import Match, Pattern
from typing import Any, Type, TypeVar

import requests
from app_logger import Logger, get_logger
from jsonpath_ng.ext import parse

from ingestor.connectors.connector_base import ConnectorBase
from ingestor.models import (
    PrincipalAttributeDio,
    PrincipalDio,
    ResourceAttributeDio,
    ResourceDio,
)

from .om_connector_config import OmConnectorConfig

logger: Logger = get_logger("ingestor.connectors.om_connector")
T = TypeVar("T")


class OmConnector(ConnectorBase):
    CONNECTOR_NAME: str = "om"

    def __init__(self):
        super().__init__()
        self.config: OmConnectorConfig = OmConnectorConfig.load()
        self.principal_source_data: list[dict[str, Any]] = []
        self.principal_attribute_source_data: list[dict[str, Any]] = []
        self.resource_source_data: list[dict[str, Any]] = []
        self.resource_attribute_source_data: list[dict[str, Any]] = []
        self._principals_loaded: bool = False
        self._principal_attributes_loaded: bool = False
        self._resources_loaded: bool = False
        self._resource_attributes_loaded: bool = False
        logger.info("Created OpenMetadata connector")

    @staticmethod
    def _join_url(base_url: str, endpoint: str) -> str:
        return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    def _get_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {
            "Content-Type": "application/json",
        }

        if self.config.auth_token:
            auth_value: str = (
                f"{self.config.auth_header_prefix} {self.config.auth_token}".strip()
                if self.config.auth_header_prefix
                else self.config.auth_token
            )
            headers[self.config.auth_header_name] = auth_value
        return headers

    @staticmethod
    def _extract_jsonpath_values(
        content_pattern: str, response_json: dict[str, Any]
    ) -> list[Any]:
        matches = parse(content_pattern).find(response_json)
        return [m.value for m in matches]

    def _fetch_entities(
        self,
        endpoint: str | None,
        query_params: str,
        content_pattern: str,
        after_jsonpath: str,
    ) -> list[dict[str, Any]]:
        if not endpoint:
            return []
        if not self.config.base_url:
            raise ValueError("om_connector.base_url is required")

        url: str = self._join_url(self.config.base_url, endpoint)
        headers: dict[str, str] = self._get_headers()
        params: dict[str, str] = OmConnectorConfig.parse_query_params(query_params)
        if "limit" not in params:
            params["limit"] = str(self.config.page_size)

        after_token: str | None = None
        seen_after_tokens: set[str] = set()
        entities: list[dict[str, Any]] = []

        while True:
            page_params: dict[str, str] = dict(params)
            if after_token:
                page_params["after"] = after_token

            response = requests.get(
                url=url,
                headers=headers,
                params=page_params,
                verify=self.config.ssl_verify,
                cert=self.config.certificate_path,
                timeout=self.config.timeout_s,
            )
            response.raise_for_status()
            response_json: dict[str, Any] = response.json()

            content = self._extract_jsonpath_values(content_pattern, response_json)
            entities.extend([c for c in content if isinstance(c, dict)])

            next_after_values: list[Any] = self._extract_jsonpath_values(
                after_jsonpath, response_json
            )
            if not next_after_values or not next_after_values[0]:
                break

            after_token = str(next_after_values[0])
            if after_token in seen_after_tokens:
                logger.warning(
                    "OpenMetadata pagination token repeated for endpoint %s; stopping pagination to avoid loop",
                    endpoint,
                )
                break
            seen_after_tokens.add(after_token)

        return entities

    @staticmethod
    def _populate_object_from_json(
        json_obj: dict[str, Any],
        attribute_mapping: dict[str, Any],
        target_class: Type[T],
    ) -> T:
        target_class_args: dict[str, Any] = {}

        for target_attr, mapped_value in attribute_mapping.items():
            target_class_args[target_attr] = None
            try:
                json_path: str = mapped_value.jsonpath
                regex: str = mapped_value.regex
            except AttributeError:
                logger.debug(
                    "Invalid attribute mapping for %s; expected jsonpath/regex tuple",
                    target_attr,
                )
                continue

            if not json_path:
                continue

            matches = parse(json_path).find(json_obj)
            if not matches:
                continue

            parsed_values: list[Any] = []
            regex_pattern: Pattern[str] | None = re.compile(regex) if regex else None

            for match in matches:
                value: Any = match.value
                if value is None:
                    continue

                if not regex_pattern:
                    parsed_values.append(value)
                    continue

                regex_match: Match[str] | None = re.match(
                    regex_pattern, value if isinstance(value, str) else str(value)
                )
                if not regex_match:
                    continue

                if regex_match.groupdict():
                    parsed_values.append(regex_match.groupdict())
                elif regex_match.groups():
                    parsed_values.append(
                        regex_match.group(1)
                        if len(regex_match.groups()) == 1
                        else regex_match.groups()
                    )
                else:
                    parsed_values.append(regex_match.group(0))

            if parsed_values:
                target_class_args[target_attr] = (
                    parsed_values if len(parsed_values) > 1 else parsed_values[0]
                )

        target_class_attr_names: list[str] = (
            [f.name for f in fields(target_class)]
            if is_dataclass(target_class)
            else list(target_class.__dict__.keys())
        )
        for attr_name in target_class_attr_names:
            if attr_name not in target_class_args:
                target_class_args[attr_name] = None

        return target_class(**target_class_args)

    @staticmethod
    def _normalise_attribute_value(value: Any) -> str:
        if isinstance(value, str):
            return value
        if value is None:
            return ""
        if isinstance(value, (int, float, bool)):
            return str(value)
        return json.dumps(value, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _normalise_attribute_entry(entry: Any) -> tuple[str, str] | None:
        if isinstance(entry, dict) and "key" in entry and "value" in entry:
            return (
                OmConnector._normalise_attribute_value(entry["key"]),
                OmConnector._normalise_attribute_value(entry["value"]),
            )

        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            return (
                OmConnector._normalise_attribute_value(entry[0]),
                OmConnector._normalise_attribute_value(entry[1]),
            )

        if isinstance(entry, str):
            if ":" in entry:
                key, value = entry.split(":", 1)
                return key, value
            if "=" in entry:
                key, value = entry.split("=", 1)
                return key, value

        return None

    def _load_principal_data(self) -> None:
        if self._principals_loaded:
            return
        self.principal_source_data = self._fetch_entities(
            endpoint=self.config.principal_endpoint,
            query_params=self.config.principal_query_params,
            content_pattern=self.config.principal_content_pattern,
            after_jsonpath=self.config.principal_after_jsonpath,
        )
        self._principals_loaded = True
        logger.info(
            "Loaded %d principal records from OpenMetadata",
            len(self.principal_source_data),
        )

    def _load_principal_attribute_data(self) -> None:
        if self._principal_attributes_loaded:
            return
        if self.config.principal_attribute_endpoint:
            self.principal_attribute_source_data = self._fetch_entities(
                endpoint=self.config.principal_attribute_endpoint,
                query_params=self.config.principal_attribute_query_params,
                content_pattern=self.config.principal_attribute_content_pattern,
                after_jsonpath=self.config.principal_attribute_after_jsonpath,
            )
        else:
            self._load_principal_data()
            self.principal_attribute_source_data = self.principal_source_data

        self._principal_attributes_loaded = True
        logger.info(
            "Loaded %d principal attribute source records from OpenMetadata",
            len(self.principal_attribute_source_data),
        )

    def _load_resource_data(self) -> None:
        if self._resources_loaded:
            return
        endpoints: list[str] = OmConnectorConfig.parse_csv(self.config.resource_endpoints)
        if not endpoints:
            endpoints = [self.config.resource_endpoint]

        combined_resources: list[dict[str, Any]] = []
        for endpoint in endpoints:
            fetched_resources = self._fetch_entities(
                endpoint=endpoint,
                query_params=self.config.resource_query_params,
                content_pattern=self.config.resource_content_pattern,
                after_jsonpath=self.config.resource_after_jsonpath,
            )
            inferred_type: str | None = self._infer_resource_type_from_endpoint(endpoint)
            if inferred_type:
                for entity in fetched_resources:
                    if "_moat_object_type" not in entity:
                        entity["_moat_object_type"] = inferred_type
            combined_resources.extend(fetched_resources)

        self.resource_source_data = combined_resources
        self._resources_loaded = True
        logger.info(
            "Loaded %d resource records from OpenMetadata",
            len(self.resource_source_data),
        )

    def _load_resource_attribute_data(self) -> None:
        if self._resource_attributes_loaded:
            return
        if self.config.resource_attribute_endpoint:
            self.resource_attribute_source_data = self._fetch_entities(
                endpoint=self.config.resource_attribute_endpoint,
                query_params=self.config.resource_attribute_query_params,
                content_pattern=self.config.resource_attribute_content_pattern,
                after_jsonpath=self.config.resource_attribute_after_jsonpath,
            )
        else:
            self._load_resource_data()
            self.resource_attribute_source_data = self.resource_source_data

        self._resource_attributes_loaded = True
        logger.info(
            "Loaded %d resource attribute source records from OpenMetadata",
            len(self.resource_attribute_source_data),
        )

    def acquire_data(self, platform: str) -> None:
        self.platform = platform
        self.principal_source_data = []
        self.principal_attribute_source_data = []
        self.resource_source_data = []
        self.resource_attribute_source_data = []
        self._principals_loaded = False
        self._principal_attributes_loaded = False
        self._resources_loaded = False
        self._resource_attributes_loaded = False

    @staticmethod
    def _infer_resource_type_from_endpoint(endpoint: str | None) -> str | None:
        if not endpoint:
            return None
        normalised_endpoint = endpoint.strip().lower()
        if normalised_endpoint.endswith("/databases"):
            return "database"
        if normalised_endpoint.endswith("/databaseschemas"):
            return "schema"
        if normalised_endpoint.endswith("/tables"):
            return "table"
        if normalised_endpoint.endswith("/topics"):
            return "topic"
        if normalised_endpoint.endswith("/dashboards"):
            return "dashboard"
        return None

    @staticmethod
    def _normalise_resource_object_type(
        object_type: str | None, resource_type_mapping: dict[str, str]
    ) -> str | None:
        if not object_type:
            return None
        key = object_type.strip().lower()
        key = key.replace("-", "_")
        return resource_type_mapping.get(key, key)

    def get_principals(self) -> list[PrincipalDio]:
        self._load_principal_data()
        principals: list[PrincipalDio] = []
        attribute_mapping: dict[str, Any] = OmConnectorConfig.attribute_jsonpath_mapping(
            prefix="principal",
            attributes_to_map=[f.name for f in fields(PrincipalDio)],
        )

        for principal in self.principal_source_data:
            obj: PrincipalDio = self._populate_object_from_json(
                json_obj=principal,
                attribute_mapping=attribute_mapping,
                target_class=PrincipalDio,
            )
            obj.platform = self.platform
            principals.append(obj)
        return principals

    def get_principal_attributes(self) -> list[PrincipalAttributeDio]:
        self._load_principal_attribute_data()
        principal_attributes: list[PrincipalAttributeDio] = []

        @dataclass
        class PrincipalMultipleAttributes:
            fq_name: str
            attributes_multi: Any

        attribute_mapping: dict[str, Any] = OmConnectorConfig.attribute_jsonpath_mapping(
            prefix="principal_attribute",
            attributes_to_map=[f.name for f in fields(PrincipalMultipleAttributes)],
        )

        for principal in self.principal_attribute_source_data:
            obj: PrincipalMultipleAttributes = self._populate_object_from_json(
                json_obj=principal,
                attribute_mapping=attribute_mapping,
                target_class=PrincipalMultipleAttributes,
            )
            if not obj.fq_name or obj.attributes_multi is None:
                continue

            multi_attrs: list[Any]
            if isinstance(obj.attributes_multi, list):
                multi_attrs = obj.attributes_multi
            else:
                multi_attrs = [obj.attributes_multi]

            for attr_entry in multi_attrs:
                parsed_attribute = self._normalise_attribute_entry(attr_entry)
                if not parsed_attribute:
                    continue

                attribute_key, attribute_value = parsed_attribute
                principal_attributes.append(
                    PrincipalAttributeDio(
                        fq_name=obj.fq_name,
                        platform=self.platform,
                        attribute_key=attribute_key,
                        attribute_value=attribute_value,
                    )
                )

        return principal_attributes

    def get_resources(self) -> list[ResourceDio]:
        self._load_resource_data()
        resources: list[ResourceDio] = []
        resource_type_mapping: dict[str, str] = (
            OmConnectorConfig.parse_resource_type_mapping(
                self.config.resource_type_mapping
            )
        )
        attribute_mapping: dict[str, Any] = OmConnectorConfig.attribute_jsonpath_mapping(
            prefix="resource",
            attributes_to_map=[f.name for f in fields(ResourceDio)],
        )

        for resource in self.resource_source_data:
            obj: ResourceDio = self._populate_object_from_json(
                json_obj=resource,
                attribute_mapping=attribute_mapping,
                target_class=ResourceDio,
            )
            if not obj.object_type:
                obj.object_type = (
                    resource.get("_moat_object_type")
                    or self.config.resource_object_type_default
                )
            obj.object_type = self._normalise_resource_object_type(
                obj.object_type, resource_type_mapping
            )
            if not obj.object_type:
                logger.warning(
                    "Skipping resource %s because object_type could not be derived",
                    obj.fq_name,
                )
                continue
            obj.platform = self.platform
            resources.append(obj)

        return resources

    def get_resource_attributes(self) -> list[ResourceAttributeDio]:
        self._load_resource_attribute_data()
        resource_attributes: list[ResourceAttributeDio] = []

        @dataclass
        class ResourceMultipleAttributes:
            fq_name: str
            attributes_multi: Any

        attribute_mapping: dict[str, Any] = OmConnectorConfig.attribute_jsonpath_mapping(
            prefix="resource_attribute",
            attributes_to_map=[f.name for f in fields(ResourceMultipleAttributes)],
        )

        for resource in self.resource_attribute_source_data:
            obj: ResourceMultipleAttributes = self._populate_object_from_json(
                json_obj=resource,
                attribute_mapping=attribute_mapping,
                target_class=ResourceMultipleAttributes,
            )
            if not obj.fq_name or obj.attributes_multi is None:
                continue

            multi_attrs: list[Any]
            if isinstance(obj.attributes_multi, list):
                multi_attrs = obj.attributes_multi
            else:
                multi_attrs = [obj.attributes_multi]

            for attr_entry in multi_attrs:
                parsed_attribute = self._normalise_attribute_entry(attr_entry)
                if not parsed_attribute:
                    continue

                attribute_key, attribute_value = parsed_attribute
                resource_attributes.append(
                    ResourceAttributeDio(
                        fq_name=obj.fq_name,
                        platform=self.platform,
                        attribute_key=attribute_key,
                        attribute_value=attribute_value,
                    )
                )

        return resource_attributes
