import re
from re import Pattern, Match
from typing import TypeVar, Type, Any
from jsonpath_ng.ext import parse
import requests
from app_logger import Logger, get_logger
from ingestor.connectors.connector_base import ConnectorBase
from ingestor.models import (
    PrincipalAttributeDio,
    PrincipalDio,
)
from dataclasses import fields, is_dataclass, dataclass
from .http_connector_config import HttpConnectorConfig

logger: Logger = get_logger("ingestor.connectors.http_connector")
T = TypeVar("T")


class HttpConnector(ConnectorBase):
    CONNECTOR_NAME: str = "http"

    AUTH_API_KEY: str = "api-key"
    AUTH_BASIC: str = "basic"
    AUTH_NONE: str = "none"
    AUTH_OAUTH2: str = "oauth2"

    def __init__(self):
        super().__init__()
        self.config: HttpConnectorConfig = self._get_config()
        logger.info("Created HTTP connector")
        self.source_data: list[dict] = []

        assert self.config.auth_method in [
            HttpConnector.AUTH_API_KEY,
            HttpConnector.AUTH_BASIC,
            HttpConnector.AUTH_NONE,
            HttpConnector.AUTH_OAUTH2,
        ], "Only API Key, Basic, Oauth2, or no auth are currently supported"

    @staticmethod
    def _get_config() -> HttpConnectorConfig:
        return HttpConnectorConfig.load()

    @staticmethod
    def _populate_object_from_json(
        json_obj: dict,
        attribute_mapping: dict,
        target_class: Type[T],
    ) -> T:
        target_class_args: dict[str, Any] = {}
        for target_attr, v in attribute_mapping.items():
            target_class_args[target_attr] = None
            try:
                json_path: str = v.jsonpath
                regex: str = v.regex or r".*"
            except AttributeError:
                logger.debug(
                    f"Error getting json_path or regex from attribute mapping for: {target_attr}"
                )
                continue
            if not json_path:
                logger.debug(f"No jsonpath for attribute: {target_attr}")
                continue
            json_path_expr = parse(json_path)
            regex_pattern: Pattern[str] = re.compile(regex)
            matches = json_path_expr.find(json_obj)
            if not matches:
                logger.info(f"No jsonpath match for attribute: {target_attr}")
                continue
            target_attr_value: list[str] = []
            for m in matches:
                regex_match: Match[str] = re.match(regex_pattern, m.value)
                if not regex_match:
                    logger.debug(f"could not match regex for attribute: {target_attr}")
                    continue
                target_attr_value.append(
                    regex_match.groupdict()
                    if len(regex_match.groups()) > 0
                    else regex_match.group(0)
                )
            if not target_attr_value:
                logger.debug(f"No regex match for attribute: {target_attr}")
                continue
            target_class_args[target_attr] = (
                target_attr_value if len(matches) > 1 else target_attr_value[0]
            )
        logger.debug(
            f"Populating {target_class.__name__} with attributes: {target_class_args}"
        )
        target_class_attr_names: list[str] = (
            [f.name for f in fields(target_class)]
            if is_dataclass(target_class)
            else target_class.__dict__.keys()
        )

        if len(target_class_attr_names) > len(target_class_args.keys()):
            logger.warning(
                f"Missing attributes for {target_class.__name__}: {target_class_attr_names - target_class_args.keys()}"
                f"adding None values"
            )
            for k in target_class_attr_names - target_class_args.keys():
                target_class_args[k] = None

        target_object = target_class(**target_class_args)
        logger.debug(
            f"Populated {target_class.__name__} with attributes: {target_class_args}"
        )

        return target_object

    def _get_access_token(self) -> str:
        access_token_endpoint: str = self.config.oauth2_endpoint
        client_id: str = self.config.oauth2_client_id
        client_secret: str = self.config.oauth2_client_secret
        grant_type: str = self.config.oauth2_grant_type
        scope: str = self.config.oauth2_scope
        logger.debug(f"Getting access token from {access_token_endpoint}")
        request_data: str = (
            f"grant_type={grant_type}&client_id={client_id}&client_secret={client_secret}&scope={scope}"
        )
        response = requests.post(
            url=access_token_endpoint,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=request_data,
            verify=self.config.ssl_verify,
            cert=self.config.certificate_path,
        )
        response.raise_for_status()
        try:
            access_token: str = response.json()["access_token"]
        except KeyError:
            logger.error(f"Error getting access token from response: {response.json()}")
            raise RuntimeError(
                f"Error getting access token from response of oauth2 endpoint: {access_token_endpoint}"
            )
        return access_token

    def acquire_data(self, platform: str) -> None:
        self.platform = platform
        auth: tuple[str, str] | None = None
        headers = {
            "Content-Type": "application/json",
        }
        """
        Add oath client_id and client_secret to headers if using API Key auth
        """
        if self.config.auth_method == HttpConnector.AUTH_BASIC:
            auth = (self.config.username, self.config.password)
        elif self.config.auth_method == HttpConnector.AUTH_API_KEY:
            headers = headers | {
                "Authorization": f"Bearer {self.config.api_key}",
            }
        elif self.config.auth_method == HttpConnector.AUTH_OAUTH2:
            headers = headers | {
                "Authorization": f"Bearer {self._get_access_token()}",
            }

        response = requests.get(
            url=self.config.url,
            headers=headers,
            auth=auth,
            verify=self.config.ssl_verify,
            cert=self.config.certificate_path,
        )

        self.source_data = response.json()
        logger.info(f"Retrieved {len(self.source_data)} records from source")
        logger.debug(f"Source data: {self.source_data}")

    def get_principals(self) -> list[PrincipalDio]:
        principals: list[PrincipalDio] = []

        attribute_mapping: dict = HttpConnectorConfig.attribute_jsonpath_mapping(
            prefix="principal", attributes_to_map=[f.name for f in fields(PrincipalDio)]
        )
        for principal in self.source_data:
            logger.debug(f"Principal: {principal}")
            obj: PrincipalDio = self._populate_object_from_json(
                json_obj=principal,
                attribute_mapping=attribute_mapping,
                target_class=PrincipalDio,
            )
            obj.platform = self.platform
            principals.append(obj)

        return principals

    def get_principal_attributes(self) -> list[PrincipalAttributeDio]:
        principal_attributes: list[PrincipalAttributeDio] = []

        @dataclass
        class PrincipalMultipleAttributes:
            fq_name: str
            attributes_multi: list[Any]

        attribute_mapping: dict = HttpConnectorConfig.attribute_jsonpath_mapping(
            prefix="principal_attribute",
            attributes_to_map=[f.name for f in fields(PrincipalMultipleAttributes)],
        )

        for principal in self.source_data:
            logger.debug(f"Principal Attribute: {principal}")
            obj: PrincipalMultipleAttributes = self._populate_object_from_json(
                json_obj=principal,
                attribute_mapping=attribute_mapping,
                target_class=PrincipalMultipleAttributes,
            )
            merged = {}
            if isinstance(obj.attributes_multi, dict):
                obj.attributes_multi = [obj.attributes_multi]
            for item in obj.attributes_multi:
                key = item["key"]
                value = item["value"]
                if key in merged:
                    merged[key] = f"{merged[key]},{value}"
                else:
                    merged[key] = value

            for k, v in merged.items():
                principal_attribute: PrincipalAttributeDio = PrincipalAttributeDio(
                    fq_name=obj.fq_name,
                    platform=self.platform,
                    attribute_key=k,
                    attribute_value=v,
                )
                principal_attributes.append(principal_attribute)

        return principal_attributes
