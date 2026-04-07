from unittest import mock

from app_config import AppConfigModelBase
from ingestor.models import (
    PrincipalAttributeDio,
    PrincipalDio,
    ResourceAttributeDio,
    ResourceDio,
)

from ..src.om_connector import OmConnector
from ..src.om_connector_config import OmConnectorConfig


def _mock_response(payload: dict):
    response = mock.Mock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def test_get_principals_paginates_and_maps_fields():
    connector = OmConnector()
    connector.acquire_data(platform="openmetadata")

    config = OmConnectorConfig()
    config.base_url = "http://openmetadata.example"
    config.auth_token = "token-value"
    config.principal_endpoint = "/api/v1/users"
    config.principal_query_params = "limit=1&fields=profile,email"
    connector.config = config

    with mock.patch("requests.get") as requests_get_mock, mock.patch.object(
        AppConfigModelBase,
        "_load_yaml_file",
        return_value={
            "om_connector.principal_fq_name_jsonpath": "$.name",
            "om_connector.principal_fq_name_regex": ".*",
            "om_connector.principal_first_name_jsonpath": "$.profile.firstName",
            "om_connector.principal_first_name_regex": ".*",
            "om_connector.principal_last_name_jsonpath": "$.profile.lastName",
            "om_connector.principal_last_name_regex": ".*",
            "om_connector.principal_user_name_jsonpath": "$.name",
            "om_connector.principal_user_name_regex": ".*",
            "om_connector.principal_email_jsonpath": "$.email",
            "om_connector.principal_email_regex": ".*",
        },
    ):
        requests_get_mock.side_effect = [
            _mock_response(
                {
                    "data": [
                        {
                            "name": "alice",
                            "profile": {"firstName": "Alice", "lastName": "Jones"},
                            "email": "alice@example.com",
                        }
                    ],
                    "paging": {"after": "cursor-1"},
                }
            ),
            _mock_response(
                {
                    "data": [
                        {
                            "name": "bob",
                            "profile": {"firstName": "Bob", "lastName": "Lee"},
                            "email": "bob@example.com",
                        }
                    ],
                    "paging": {},
                }
            ),
        ]

        principals: list[PrincipalDio] = connector.get_principals()

    assert principals == [
        PrincipalDio(
            fq_name="alice",
            platform="openmetadata",
            first_name="Alice",
            last_name="Jones",
            user_name="alice",
            email="alice@example.com",
        ),
        PrincipalDio(
            fq_name="bob",
            platform="openmetadata",
            first_name="Bob",
            last_name="Lee",
            user_name="bob",
            email="bob@example.com",
        ),
    ]

    assert requests_get_mock.call_count == 2
    first_call = requests_get_mock.call_args_list[0].kwargs
    second_call = requests_get_mock.call_args_list[1].kwargs
    assert first_call["url"] == "http://openmetadata.example/api/v1/users"
    assert first_call["params"] == {"limit": "1", "fields": "profile,email"}
    assert first_call["headers"]["Authorization"] == "Bearer token-value"
    assert second_call["params"] == {
        "limit": "1",
        "fields": "profile,email",
        "after": "cursor-1",
    }


def test_get_principal_attributes_from_principal_payload():
    connector = OmConnector()
    connector.acquire_data(platform="openmetadata")

    config = OmConnectorConfig()
    config.base_url = "http://openmetadata.example"
    config.principal_endpoint = "/api/v1/users"
    config.principal_query_params = "limit=10"
    config.principal_attribute_endpoint = None
    connector.config = config

    with mock.patch("requests.get") as requests_get_mock, mock.patch.object(
        AppConfigModelBase,
        "_load_yaml_file",
        return_value={
            "om_connector.principal_attribute_fq_name_jsonpath": "$.name",
            "om_connector.principal_attribute_fq_name_regex": ".*",
            "om_connector.principal_attribute_attributes_multi_jsonpath": "$.teams[*]",
            "om_connector.principal_attribute_attributes_multi_regex": "(?P<key>[^:]+):(?P<value>.+)",
        },
    ):
        requests_get_mock.return_value = _mock_response(
            {
                "data": [
                    {
                        "name": "alice",
                        "teams": ["team:data-platform", "dept:analytics"],
                    }
                ],
                "paging": {},
            }
        )

        principal_attributes: list[PrincipalAttributeDio] = (
            connector.get_principal_attributes()
        )

    assert principal_attributes == [
        PrincipalAttributeDio(
            fq_name="alice",
            platform="openmetadata",
            attribute_key="team",
            attribute_value="data-platform",
        ),
        PrincipalAttributeDio(
            fq_name="alice",
            platform="openmetadata",
            attribute_key="dept",
            attribute_value="analytics",
        ),
    ]
    requests_get_mock.assert_called_once()


def test_get_resources_and_attributes_reuse_loaded_resource_data():
    connector = OmConnector()
    connector.acquire_data(platform="openmetadata")

    config = OmConnectorConfig()
    config.base_url = "http://openmetadata.example"
    config.resource_endpoint = "/api/v1/tables"
    config.resource_query_params = "limit=100&fields=tags"
    config.resource_attribute_endpoint = None
    connector.config = config

    with mock.patch("requests.get") as requests_get_mock, mock.patch.object(
        AppConfigModelBase,
        "_load_yaml_file",
        return_value={
            "om_connector.resource_fq_name_jsonpath": "$.fullyQualifiedName",
            "om_connector.resource_fq_name_regex": ".*",
            "om_connector.resource_object_type_jsonpath": "$.entityType",
            "om_connector.resource_object_type_regex": ".*",
            "om_connector.resource_attribute_fq_name_jsonpath": "$.fullyQualifiedName",
            "om_connector.resource_attribute_fq_name_regex": ".*",
            "om_connector.resource_attribute_attributes_multi_jsonpath": "$.tags[*].tagFQN",
            "om_connector.resource_attribute_attributes_multi_regex": "(?P<key>[^.]+)\\.(?P<value>.+)",
        },
    ):
        requests_get_mock.return_value = _mock_response(
            {
                "data": [
                    {
                        "fullyQualifiedName": "service.db.schema.orders",
                        "entityType": "table",
                        "tags": [
                            {"tagFQN": "Tier.Tier1"},
                            {"tagFQN": "PII.Sensitive"},
                        ],
                    }
                ],
                "paging": {},
            }
        )

        resources: list[ResourceDio] = connector.get_resources()
        resource_attributes: list[ResourceAttributeDio] = (
            connector.get_resource_attributes()
        )

    assert resources == [
        ResourceDio(
            fq_name="service.db.schema.orders",
            object_type="table",
            platform="openmetadata",
        )
    ]
    assert resource_attributes == [
        ResourceAttributeDio(
            fq_name="service.db.schema.orders",
            platform="openmetadata",
            attribute_key="Tier",
            attribute_value="Tier1",
        ),
        ResourceAttributeDio(
            fq_name="service.db.schema.orders",
            platform="openmetadata",
            attribute_key="PII",
            attribute_value="Sensitive",
        ),
    ]
    requests_get_mock.assert_called_once()


def test_get_resources_supports_multiple_endpoints_with_inferred_object_types():
    connector = OmConnector()
    connector.acquire_data(platform="openmetadata")

    config = OmConnectorConfig()
    config.base_url = "http://openmetadata.example"
    config.resource_endpoints = (
        "/api/v1/databases,/api/v1/databaseSchemas,/api/v1/tables"
    )
    config.resource_query_params = "limit=100"
    config.resource_type_mapping = ""
    connector.config = config

    with mock.patch("requests.get") as requests_get_mock, mock.patch.object(
        AppConfigModelBase,
        "_load_yaml_file",
        return_value={
            "om_connector.resource_fq_name_jsonpath": "$.fullyQualifiedName",
            "om_connector.resource_fq_name_regex": ".*",
        },
    ):
        requests_get_mock.side_effect = [
            _mock_response(
                {
                    "data": [
                        {"fullyQualifiedName": "sample_service.sample_catalog"},
                    ],
                    "paging": {},
                }
            ),
            _mock_response(
                {
                    "data": [
                        {
                            "fullyQualifiedName": "sample_service.sample_catalog.sample_schema"
                        },
                    ],
                    "paging": {},
                }
            ),
            _mock_response(
                {
                    "data": [
                        {
                            "fullyQualifiedName": "sample_service.sample_catalog.sample_schema.orders"
                        },
                    ],
                    "paging": {},
                }
            ),
        ]

        resources: list[ResourceDio] = connector.get_resources()

    assert resources == [
        ResourceDio(
            fq_name="sample_service.sample_catalog",
            object_type="database",
            platform="openmetadata",
        ),
        ResourceDio(
            fq_name="sample_service.sample_catalog.sample_schema",
            object_type="schema",
            platform="openmetadata",
        ),
        ResourceDio(
            fq_name="sample_service.sample_catalog.sample_schema.orders",
            object_type="table",
            platform="openmetadata",
        ),
    ]
    assert requests_get_mock.call_count == 3


def test_get_resources_normalises_table_type_values():
    connector = OmConnector()
    connector.acquire_data(platform="openmetadata")

    config = OmConnectorConfig()
    config.base_url = "http://openmetadata.example"
    config.resource_endpoint = "/api/v1/tables"
    config.resource_query_params = "limit=100&fields=tableType"
    connector.config = config

    with mock.patch("requests.get") as requests_get_mock, mock.patch.object(
        AppConfigModelBase,
        "_load_yaml_file",
        return_value={
            "om_connector.resource_fq_name_jsonpath": "$.fullyQualifiedName",
            "om_connector.resource_fq_name_regex": ".*",
            "om_connector.resource_object_type_jsonpath": "$.tableType",
            "om_connector.resource_object_type_regex": ".*",
        },
    ):
        requests_get_mock.return_value = _mock_response(
            {
                "data": [
                    {
                        "fullyQualifiedName": "sample_service.sample_catalog.sample_schema.orders_mv",
                        "tableType": "MaterializedView",
                    }
                ],
                "paging": {},
            }
        )

        resources: list[ResourceDio] = connector.get_resources()

    assert resources == [
        ResourceDio(
            fq_name="sample_service.sample_catalog.sample_schema.orders_mv",
            object_type="table",
            platform="openmetadata",
        )
    ]
