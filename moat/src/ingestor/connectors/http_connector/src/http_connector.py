from typing import TypeVar, Type
from jsonpath_ng import parse
import requests
from app_logger import Logger, get_logger
from ingestor.connectors.connector_base import ConnectorBase
from ingestor.models import (
    PrincipalAttributeDio,
    PrincipalDio,
)

from .http_connnector_config import HttpConnectorConfig

logger: Logger = get_logger("ingestor.connectors.http_connector")


class HttpConnector(ConnectorBase):
    T = TypeVar("T")
    CONNECTOR_NAME: str = "http"

    AUTH_API_KEY: str = "api-key"
    AUTH_BASIC: str = "basic"
    AUTH_NONE: str = "none"

    def __init__(self):
        super().__init__()
        self.config: HttpConnectorConfig = self._get_config()
        logger.info("Created HTTP connector")
        self.source_data: list[dict] = []

        assert self.config.auth_method in [
            HttpConnector.AUTH_API_KEY,
            HttpConnector.AUTH_BASIC,
            HttpConnector.AUTH_NONE,
        ], "Only API Key Basic or no auth are currently supported"

    @staticmethod
    def _get_config() -> HttpConnectorConfig:
        return HttpConnectorConfig.load()

    @staticmethod
    def _populate_object_from_json_using_jsonpath_mapping(
        json_obj: dict,
        json_path_mapping: dict,
        target_object: Type[T],
    ) -> Type[T]:

        for target_attr, json_path in json_path_mapping.items():
            json_path_expr = parse(json_path)
            matches = json_path_expr.find(json_obj)
            if matches:
                setattr(target_object, target_attr, matches[0].value)

        return target_object

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
        for principal in self.source_data:
            logger.debug(f"Principal: {principal}")
            self._populate_object_from_json_using_jsonpath_mapping(
                json_obj=principal,
                json_path_mapping=self.config.principals_jsonpath_mapping(),
                target_object=PrincipalDio,
            )
            principals.append(
                self._populate_object_from_json_using_jsonpath_mapping(
                    json_obj=principal,
                    json_path_mapping=self.config.principals_jsonpath_mapping()
                    | {"platform": self.platform},
                    target_object=PrincipalDio,
                )
            )

        return principals

    def get_principal_attributes(self) -> list[PrincipalAttributeDio]:

        return []
