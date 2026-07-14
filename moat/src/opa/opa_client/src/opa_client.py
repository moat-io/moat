import requests
from app_logger import Logger, get_logger
from opa.models.src.opa_bundle_response_model import OpaBundleResponse
from opa.models.src.opa_query_model import OpaQueryModel
from opa.models.src.opa_query_response_model import OpaQueryResponseModel
from opa.models.src.opa_request_model import OpaRequestModel
from opa.models.src.opa_response_model import OpaResponseModel
from opa.models.src.trino_opa_request_model import OpaTableResource
from pydantic import BaseModel

from .opa_client_config import OpaClientConfig

logger: Logger = get_logger("opa_client")


class OpaClient:
    def __init__(self):
        self.config = OpaClientConfig().load()

    def _get_opa_url(self, path: str | None = None) -> str:
        return f"{self.config.scheme}://{self.config.hostname}:{self.config.port}{path or self.config.path}"

    def put_policy(
        self,
        policy_name: str,
        policy_content: str | None = None,
        policy_file_path: str | None = None,
    ) -> None:
        url: str = self._get_opa_url(path=f"/v1/policies/{policy_name}")

        if policy_file_path:
            with open(policy_file_path) as f:
                policy_content = f.read()

        try:
            logger.info(f"PUT Policy: OPA request: url: {url}")
            response: requests.Response = requests.put(
                url=url, data=policy_content, timeout=float(self.config.timeout_seconds)
            )
            logger.info(
                f"PUT Policy: OPA response: status code: {response.status_code} Body: {response.text}"
            )
        except requests.RequestException as e:
            logger.exception(f"Unexpected error pushing policy to OPA: {e}")

    def authorise_request(self, request_model: BaseModel) -> bool | None:
        opa_request: OpaRequestModel = OpaRequestModel(input=request_model.dict())
        return self._send_opa_authorize_request(
            url=self._get_opa_url(), opa_request=opa_request
        )

    def filter_table(
        self, username: str, database: str, schema: str, table: str
    ) -> bool | None:
        opa_request: OpaRequestModel = OpaRequestModel(
            input={
                "action": {
                    "operation": "FilterTables",
                    "resource": {
                        "table": {
                            "catalogName": database,
                            "schemaName": schema,
                            "tableName": table,
                        }
                    },
                },
                "context": {
                    "identity": {"user": username},
                    "softwareStack": {"Version": "0.1.0"},
                },
            }
        )
        return self._send_opa_authorize_request(
            url=self._get_opa_url(path="/v1/data/moat/trino/allow"),
            opa_request=opa_request,
        )

    # TODO move to authz provider
    def filter_schema(self, username: str, database: str, schema: str) -> bool | None:
        opa_request: OpaRequestModel = OpaRequestModel(
            input={
                "action": {
                    "operation": "FilterSchemas",
                    "resource": {
                        "schema": {
                            "catalogName": database,
                            "schemaName": schema,
                        }
                    },
                },
                "context": {
                    "identity": {"user": username},
                    "softwareStack": {"Version": "0.1.0"},
                },
            }
        )
        return self._send_opa_authorize_request(
            url=self._get_opa_url(path="/v1/data/moat/trino/allow"),
            opa_request=opa_request,
        )

    def get_allowed_policy_actions(self, request_model: BaseModel) -> list[str]:
        opa_query: OpaQueryModel = OpaQueryModel(
            query="data.moat.authz.allowed_policy_actions = action",
            input=request_model.dict(),
        )
        # TODO the response here comes from user input so should be validated with pydantic
        opa_response: OpaQueryResponseModel | None = self._send_opa_query(
            url=self._get_opa_url(path="/v1/query"),
            opa_query=opa_query,
        )
        if opa_response is None:
            return []
        else:
            return opa_response.result[0].get("action", [])

    def filter_resources_batch(
        self, username: str, resources: list[OpaTableResource]
    ) -> list[bool]:
        """
        Batch filter resources for a given principal using Trino OPA policies.

        Args:
            username: The username of the principal
            resources: List of OpaTableResource objects

        Returns:
            List of boolean values indicating whether each resource is allowed
        """
        payload: dict = {
            "action": {
                "operation": "FilterTables",
                "filterResources": [
                    {
                        "table": {
                            "catalogName": resource.catalog,
                            "schemaName": resource.schema,
                            "tableName": resource.table,
                        }
                    }
                    for resource in resources
                ],
            },
            "context": {
                "identity": {"user": username},
            },
        }

        url = self._get_opa_url(path="/v1/data/moat/trino/batch")

        logger.info(f"OPA batch request: url: {url} batch_size: {len(resources)}")
        results: list[bool] | None = self._send_opa_batch_filter(
            url=url, payload=payload
        )

        # Deny everything if the request failed or the response was malformed.
        if results is None:
            return [False] * len(resources)
        return results

    def get_bundle_metadata(self) -> OpaBundleResponse | None:
        """
        Fetch the current bundle state from OPA, including the etag of the
        last successfully activated bundle.

        OPA records bundle metadata (manifest, etag, ...) under the
        `data.system.bundles` document, keyed by bundle name. Returns an
        OpaBundleResponse, or None if the request fails.
        """
        url: str = self._get_opa_url(path="/v1/data/system/bundles")
        try:
            logger.info(f"OPA bundle metadata request: url: {url}")
            response: requests.Response = requests.get(
                url=url, timeout=float(self.config.timeout_seconds)
            )
        except requests.RequestException as e:
            logger.exception(f"Unexpected error querying OPA bundle metadata: {e}")
            return None

        logger.info(
            f"OPA bundle metadata response: status code: {response.status_code} "
            f"Body: {response.text}"
        )
        if response.status_code != 200:
            logger.exception(
                f"OPA returned a non-200 status code: {response.status_code}"
            )
            return None
        return OpaBundleResponse(**response.json())

    def _send_opa_authorize_request(
        self, url: str, opa_request: OpaRequestModel
    ) -> bool | None:
        opa_response_dict: dict | None = self._send_opa_request(
            url=url, opa_request=opa_request
        )
        opa_response: OpaResponseModel = OpaResponseModel(**opa_response_dict)
        return opa_response.result

    def _send_opa_query(self, url, opa_query: OpaQueryModel) -> OpaQueryResponseModel:
        opa_response_dict: dict | None = self._send_opa_request(
            url=url, opa_request=opa_query
        )
        opa_response: OpaQueryResponseModel = OpaQueryResponseModel(**opa_response_dict)
        return opa_response

    def _send_opa_batch_filter(self, url, payload: dict) -> list[bool]:
        resource_count: int = len(payload["action"]["filterResources"])

        opa_response_dict: dict | None = self._send_opa_request(
            url=url, opa_request=OpaRequestModel(input=payload)
        )

        if opa_response_dict is None or "result" not in opa_response_dict:
            logger.error(f"Unexpected OPA batch response format: {opa_response_dict}")
            return [False] * resource_count

        # OPA returns the indices of the resources it allows; expand that into a
        # boolean per requested resource, preserving the input order.
        allowed_indices: set[int] = set(opa_response_dict["result"])
        resource_count: int = len(payload["action"]["filterResources"])
        return [index in allowed_indices for index in range(resource_count)]

    def _send_opa_request(
        self, url: str, opa_request: OpaRequestModel | OpaQueryModel
    ) -> dict | None:
        opa_payload: dict = opa_request.dict()
        try:
            logger.info(f"OPA request: url: {url} payload: {opa_payload}")
            response: requests.Response = requests.post(
                url=url, json=opa_payload, timeout=float(self.config.timeout_seconds)
            )
        except requests.RequestException as e:
            logger.exception(f"Unexpected error querying OPA: {e}")
            return None

        logger.info(
            f"OPA response: status code: {response.status_code} Json: {response.json()} Body: {response.text}"
        )
        if response.status_code != 200:
            logger.exception(
                f"OPA returned a non-200 status code: {response.status_code}"
            )
            return None
        return response.json()
