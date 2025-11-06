from unittest import mock
from dataclasses import dataclass
from collections import namedtuple

import pytest

from app_config import AppConfigModelBase
from ingestor.models import PrincipalDio, PrincipalAttributeDio
from ..src.http_connector import HttpConnector, HttpConnectorConfig

# Test helper for attribute mapping
AttributeMapping = namedtuple("AttributeMapping", ["jsonpath", "regex"])

source_data = [
    {
        "attributes": {
            "loginID": "onyangokariuiki",
            "displayName": "Onyango Kariuiki",
            "familyName": "kariuiki",
            "givenName": "onyango",
            "active": "true",
            "externalId": None,
            "userName": "onyangokariuiki@aol.com.io",
            "email": "onyangokariuiki@aol.com.io",
            "group": ["ReadAllNonSensitive Sales", "ADGROUP::ABCD_All_Users"],
            "urn:ietf:params:scim:enterpriseExtended.username": "onyangokariuiki",
        },
    },
    {
        "attributes": {
            "loginID": "tomtakahara",
            "displayName": "Tom Takahara",
            "familyName": "takahara",
            "givenName": "tom",
            "active": "true",
            "externalId": None,
            "userName": "tomtakahara@aol.com.io",
            "email": "tomtakahara@aol.com.io",
            "group": [
                "ReadAllNonSensitive Sales",
                "ADGROUP::ABCD_All_Users",
                "ReadAllNonSensitive Marketing",
                "ReadAllNonSensitive HT",
                "ADGROUP::ABCD_All_Admins",
                "ReadSensitive IT",
                "DP Customer Information",
            ],
            "urn:ietf:params:scim:enterpriseExtended.username": "tomtakahara",
        },
    },
]


@dataclass
class TestObjectDataclass:
    """Simple test object for unit testing"""

    name: str
    email: str
    age: int
    city: str
    group_name: str
    full_match: str


class TestObject:
    name: str
    email: str
    age: int
    city: str
    group_name: str
    full_match: str

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def test_acquire_data():
    no_auth_config = HttpConnectorConfig()
    no_auth_config.auth_method = "none"
    no_auth_config.url = "https://example.com/api/v1/things?query=true"

    basic_auth_config = HttpConnectorConfig()
    basic_auth_config.auth_method = "basic"
    basic_auth_config.username = "username"
    basic_auth_config.password = "password"
    basic_auth_config.url = "https://example.com/api/v1/things?query=true"
    basic_auth_config.ssl_verify = False

    bearer_auth_config = HttpConnectorConfig()
    bearer_auth_config.url = "https://example.com/api/v1/things?query=true"
    bearer_auth_config.auth_method = "api-key"
    bearer_auth_config.api_key = "bearer_token"

    oauth2_config = HttpConnectorConfig()
    oauth2_config.url = "https://example.com/api/v1/things?query=true"
    oauth2_config.auth_method = "oauth2"
    oauth2_config.oauth2_client_id = "client-id"
    oauth2_config.oauth2_client_secret = "client-secret"
    oauth2_config.oauth2_endpoint = "https://idp.coi/oauth2/token"
    oauth2_config.oauth2_scope = "idn:accounts:read sp:scopes:default"
    oauth2_config.oauth2_grant_type = "client_credentials"

    oauth2_config_without_scope = HttpConnectorConfig()
    oauth2_config_without_scope.url = "https://example.com/api/v1/things?query=true"
    oauth2_config_without_scope.auth_method = "oauth2"
    oauth2_config_without_scope.oauth2_client_id = "client-id"
    oauth2_config_without_scope.oauth2_client_secret = "client-secret"
    oauth2_config_without_scope.oauth2_endpoint = "https://idp.coi/oauth2/token"
    oauth2_config_without_scope.oauth2_grant_type = "client_credentials"

    with mock.patch("requests.get") as requests_get_mock, mock.patch(
        "requests.post"
    ) as requests_post_mock:
        connector = HttpConnector()
        connector.config = no_auth_config
        connector.acquire_data(platform="idp")

        requests_get_mock.assert_called_once_with(
            url="https://example.com/api/v1/things?query=true",
            headers={
                "Content-Type": "application/json",
            },
            params={},
            auth=None,
            verify=True,
            cert=None,
        )
        requests_get_mock.reset_mock()

        # basic auth
        connector.config = basic_auth_config
        connector.acquire_data(platform="idp")
        requests_get_mock.assert_called_once_with(
            url="https://example.com/api/v1/things?query=true",
            headers={
                "Content-Type": "application/json",
            },
            params={},
            auth=("username", "password"),
            verify=False,
            cert=None,
        )
        requests_get_mock.reset_mock()

        # api key auth
        connector.config = bearer_auth_config
        connector.acquire_data(platform="idp")
        requests_get_mock.assert_called_once_with(
            url="https://example.com/api/v1/things?query=true",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer bearer_token",
            },
            params={},
            auth=None,
            verify=True,
            cert=None,
        )
        # oauth2
        requests_get_mock.reset_mock()
        connector.config = oauth2_config
        requests_post_mock.return_value.json.return_value = {
            "access_token": "oauth2_access_token",
        }
        connector.acquire_data(platform="idp")
        requests_get_mock.assert_called_once_with(
            url="https://example.com/api/v1/things?query=true",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer oauth2_access_token",
            },
            params={},
            auth=None,
            verify=True,
            cert=None,
        )
        requests_post_mock.assert_called_once_with(
            url="https://idp.coi/oauth2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data="grant_type=client_credentials&client_id=client-id&client_secret=client-secret&scope=idn:accounts:read sp:scopes:default",
            verify=True,
            cert=None,
        )

        # oauth2 without scope (Scope is optional.)
        requests_get_mock.reset_mock()
        requests_post_mock.reset_mock()
        connector.config = oauth2_config_without_scope
        requests_post_mock.return_value.json.return_value = {
            "access_token": "oauth2_access_token",
        }
        connector.acquire_data(platform="idp")
        requests_get_mock.assert_called_once_with(
            url="https://example.com/api/v1/things?query=true",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer oauth2_access_token",
            },
            params={},
            auth=None,
            verify=True,
            cert=None,
        )
        requests_post_mock.assert_called_once_with(
            url="https://idp.coi/oauth2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data="grant_type=client_credentials&client_id=client-id&client_secret=client-secret",
            verify=True,
            cert=None,
        )


def test_get_principals():
    config = HttpConnectorConfig()
    connector = HttpConnector()
    connector.config = config
    connector.platform = "idp"

    connector.source_data = source_data
    with mock.patch.object(AppConfigModelBase, "_load_yaml_file") as load_yaml_mock:
        load_yaml_mock.side_effect = [
            {
                "http_connector.principal_fq_name_jsonpath": "$.attributes.loginID",
                "http_connector.principal_fq_name_regex": ".*",
                "http_connector.principal_first_name_jsonpath": "$.attributes.givenName",
                "http_connector.principal_first_name_regex": ".*",
                "http_connector.principal_last_name_jsonpath": "$.attributes.familyName",
                "http_connector.principal_last_name_regex": ".*",
                "http_connector.principal_user_name_jsonpath": "$.attributes.loginID",
                "http_connector.principal_user_name_regex": ".*",
                "http_connector.principal_email_jsonpath": "$.attributes.email",
                "http_connector.principal_email_regex": ".*",
            }
            for _ in range(12)
        ]
        principals: list[PrincipalDio] = connector.get_principals()
    assert principals == [
        # TODO add the rest of the params from PrincipalDbo to PrincipalDio
        PrincipalDio(
            fq_name="onyangokariuiki",
            platform="idp",
            first_name="onyango",
            last_name="kariuiki",
            user_name="onyangokariuiki",
            email="onyangokariuiki@aol.com.io",
        ),
        PrincipalDio(
            fq_name="tomtakahara",
            platform="idp",
            first_name="tom",
            last_name="takahara",
            user_name="tomtakahara",
            email="tomtakahara@aol.com.io",
        ),
    ]


def test_get_principals_with_attributes():
    config = HttpConnectorConfig()
    connector = HttpConnector()
    connector.config = config
    connector.platform = "idp"
    connector.source_data = source_data
    with mock.patch.object(AppConfigModelBase, "_load_yaml_file") as load_yaml_mock:
        load_yaml_mock.side_effect = [
            {
                "http_connector.principal_attribute_fq_name_jsonpath": "$.attributes.loginID",
                "http_connector.principal_attribute_fq_name_regex": ".*",
                "http_connector.principal_attribute_attributes_multi_jsonpath": "$.attributes.group[?(@ =~ '(?P<key>[a-zA-Z]+) (?P<value>[a-zA-Z0-9 ]+)')]",
                "http_connector.principal_attribute_attributes_multi_regex": r"(?P<key>[a-zA-Z]+) (?P<value>[a-zA-Z0-9 ]+)",
                "http_connector.principal_attribute_attributes_key_regex": ".*",
                "http_connector.principal_attribute_attributes_value_regex": r".*",
            }
            for _ in range(8)
        ]
        principal_attributes: list[PrincipalAttributeDio] = (
            connector.get_principal_attributes()
        )
    assert principal_attributes == [
        PrincipalAttributeDio(
            fq_name="onyangokariuiki",
            platform="idp",
            attribute_key="ReadAllNonSensitive",
            attribute_value="Sales",
        ),
        PrincipalAttributeDio(
            fq_name="tomtakahara",
            platform="idp",
            attribute_key="ReadAllNonSensitive",
            attribute_value="Sales,Marketing,HT",
        ),
        PrincipalAttributeDio(
            fq_name="tomtakahara",
            platform="idp",
            attribute_key="ReadSensitive",
            attribute_value="IT",
        ),
        PrincipalAttributeDio(
            fq_name="tomtakahara",
            platform="idp",
            attribute_key="DP",
            attribute_value="Customer Information",
        ),
    ]


def test_populate_object_from_json_jsonpath_extraction():
    """Test JSONPath extraction: basic, nested, and array access"""
    json_obj = {
        "name": "John Doe",
        "email": "john@example.com",
        "user": {"profile": {"contact": {"phone": "555-1234"}}},
        "groups": [{"name": "admin"}, {"name": "users"}],
    }

    attribute_mapping = {
        "name": AttributeMapping(jsonpath="$.name", regex=None),
        "email": AttributeMapping(jsonpath="$.email", regex=None),
        "city": AttributeMapping(jsonpath="$.user.profile.contact.phone", regex=None),
        "group_name": AttributeMapping(jsonpath="$.groups[1].name", regex=None),
    }

    result = HttpConnector._populate_object_from_json(
        json_obj=json_obj,
        attribute_mapping=attribute_mapping,
        target_class=TestObjectDataclass,
    )

    assert result.name == "John Doe"  # Basic
    assert result.email == "john@example.com"  # Basic
    assert result.city == "555-1234"  # Nested
    assert result.group_name == "users"  # Array


def test_populate_object_from_json_regex_patterns():
    """Test regex: no groups (full match), single group, multiple groups, and filtering"""
    json_obj = {
        "code": "ABC-123-XYZ",
        "dn": "cn=developers,ou=groups,dc=example,dc=com",
        "version": "v1.2.3-beta",
        "city": "New York, NY 10001",
    }

    attribute_mapping = {
        "name": AttributeMapping(jsonpath="$.code", regex=r"[A-Z]+-\d+"),  # No groups
        "group_name": AttributeMapping(
            jsonpath="$.dn", regex=r"cn=(?P<developer>[a-z_]+)"
        ),  # Single
        "email": AttributeMapping(
            jsonpath="$.version", regex=r"v(?P<one>\d+)\.(?P<two>\d+)\.(?P<three>\d+)"
        ),  # Multiple
        "city": AttributeMapping(jsonpath="$.city", regex=r"[A-Za-z ]+"),  # Filter
    }

    @dataclass
    class TestObjectGroupedDictDataclass:
        name: str
        email: dict
        age: int
        city: str
        group_name: dict
        full_match: str

    result = HttpConnector._populate_object_from_json(
        json_obj=json_obj,
        attribute_mapping=attribute_mapping,
        target_class=TestObjectGroupedDictDataclass,
    )

    assert result.name == "ABC-123"  # Full match
    assert result.group_name == {"developer": "developers"}  # Single group
    assert result.email == {
        "one": "1",
        "three": "3",
        "two": "2",
    }  # Multiple groups joined
    assert result.city == "New York"  # Filtered


def test_populate_object_from_json_error_handling():
    """Test error cases: missing paths, failed regex, missing attributes, empty inputs"""
    json_obj = {"name": "John Doe", "email": "john@example.com"}
    SimpleMapping = namedtuple("SimpleMapping", ["value"])

    attribute_mapping = {
        "name": AttributeMapping(jsonpath="$.name", regex=None),  # Valid
        "email": AttributeMapping(jsonpath="$.missing", regex=None),  # Missing path
        "age": AttributeMapping(jsonpath="$.email", regex=r"\d+"),  # Regex no match
        "city": SimpleMapping(value="test"),  # Missing jsonpath attribute
    }

    result = HttpConnector._populate_object_from_json(
        json_obj=json_obj,
        attribute_mapping=attribute_mapping,
        target_class=TestObjectDataclass,
    )

    assert result.name == "John Doe"  # Valid
    assert result.email is None  # Missing path
    assert result.age is None  # Regex no match
    assert result.city is None  # Missing attribute

    # Empty cases
    empty_result = HttpConnector._populate_object_from_json(
        json_obj={}, attribute_mapping={}, target_class=TestObjectDataclass
    )
    assert empty_result.name is None


def test_populate_object_from_json_integration_with_principal_dio():
    """Integration test with PrincipalDio and realistic LDAP-like data"""
    json_obj = {
        "attributes": {
            "givenName": "John",
            "familyName": "Doe",
            "userName": "jdoe",
            "email": "john.doe@example.com",
        }
    }

    attribute_mapping = {
        "first_name": AttributeMapping(jsonpath="$.attributes.givenName", regex=None),
        "last_name": AttributeMapping(jsonpath="$.attributes.familyName", regex=None),
        "user_name": AttributeMapping(jsonpath="$.attributes.userName", regex=None),
        "email": AttributeMapping(jsonpath="$.attributes.email", regex=None),
    }

    result = HttpConnector._populate_object_from_json(
        json_obj=json_obj,
        attribute_mapping=attribute_mapping,
        target_class=PrincipalDio,
    )

    assert isinstance(result, PrincipalDio)
    assert result.first_name == "John"
    assert result.last_name == "Doe"
    assert result.user_name == "jdoe"
    assert result.email == "john.doe@example.com"


def test_handle_response_json():
    assert [{"1": 1}, {"2": 2}] == HttpConnector.handle_response_json(
        "$[*]", [{"1": 1}, {"2": 2}]
    )
    assert [{"item": 1}, {"item": 2}] == HttpConnector.handle_response_json(
        "$.items[*]", {"items": [{"item": 1}, {"item": 2}]}
    )
    assert [] == HttpConnector.handle_response_json(
        "$.itemss[*]", {"items": [{"item": 1}, {"item": 2}]}
    )


def test_get_total_count():
    assert 1 == HttpConnector.get_total_count(
        response_header={"count": 1},
        response_json=[{"1": 1}, {"2": 2}],
        pagination_key_location="response_header",
        pagination_key_name="count",
    )

    assert 1 == HttpConnector.get_total_count(
        response_header={"counts": 1},
        response_json={"count": 1, "items": [{"1": 1}, {"2": 2}]},
        pagination_key_location="response_json",
        pagination_key_name="count",
    )

    with pytest.raises(KeyError):
        HttpConnector.get_total_count(
            response_header={"counts": 1},
            response_json={"counts": 1, "items": [{"1": 1}, {"2": 2}]},
            pagination_key_location="response_json",
            pagination_key_name="count",
        )

    with pytest.raises(AssertionError):
        HttpConnector.get_total_count(
            response_header={"counts": 1},
            response_json={"counts": 1, "items": [{"1": 1}, {"2": 2}]},
            pagination_key_location="response_jsonasd",
            pagination_key_name="count",
        )


def test_acquire_data_paginated():
    oauth2_config = HttpConnectorConfig()
    oauth2_config.url = "https://example.com/api/v1/things?query=true"
    oauth2_config.auth_method = "oauth2"
    oauth2_config.oauth2_client_id = "client-id"
    oauth2_config.oauth2_client_secret = "client-secret"
    oauth2_config.oauth2_endpoint = "https://idp.coi/oauth2/token"
    oauth2_config.oauth2_scope = "idn:accounts:read sp:scopes:default"
    oauth2_config.oauth2_grant_type = "client_credentials"
    oauth2_config.pagination_enabled = True
    oauth2_config.pagination_size = 2
    oauth2_config.pagination_target_key = "limit"
    oauth2_config.pagination_key_location = "response_header"
    oauth2_config.pagination_key_name = "X-Total-Count"
    oauth2_config.pagination_offset_key = "offset"

    with mock.patch("requests.get") as requests_get_mock, mock.patch(
        "requests.post"
    ) as requests_post_mock:
        connector = HttpConnector()
        connector.config = oauth2_config
        requests_post_mock.return_value.json.return_value = {
            "access_token": "oauth2_access_token",
        }
        requests_get_mock.side_effect = [
            mock.Mock(
                status_code=200,
                json=mock.Mock(return_value=[{"item": 1}, {"item": 2}]),
                headers={"X-Total-Count": "5"},
            ),
            mock.Mock(
                status_code=200,
                json=mock.Mock(return_value=[{"item": 3}, {"item": 4}]),
                headers={"X-Total-Count": "5"},
            ),
            mock.Mock(
                status_code=200,
                json=mock.Mock(return_value=[{"item": 5}]),
                headers={"X-Total-Count": "5"},
            ),
        ]
        connector.acquire_data(platform="idp")
        assert requests_get_mock.call_count == 3
        assert requests_get_mock.call_args_list[0][1]["params"] == {
            "limit": 2,
            "offset": 0,
        }
        assert requests_get_mock.call_args_list[1][1]["params"] == {
            "limit": 2,
            "offset": 2,
        }
        assert requests_get_mock.call_args_list[2][1]["params"] == {
            "limit": 2,
            "offset": 4,
        }
        requests_post_mock.assert_called_once_with(
            url="https://idp.coi/oauth2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data="grant_type=client_credentials&client_id=client-id&client_secret=client-secret&scope=idn:accounts:read sp:scopes:default",
            verify=True,
            cert=None,
        )
