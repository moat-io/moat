from unittest import mock

from ingestor.models import PrincipalDio
from ..src.http_connector import HttpConnector, HttpConnectorConfig


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

    with mock.patch("requests.get") as requests_mock:
        connector = HttpConnector()
        connector.config = no_auth_config
        connector.acquire_data(platform="identitynow")

        requests_mock.assert_called_once_with(
            url="https://example.com/api/v1/things?query=true",
            headers={
                "Content-Type": "application/json",
            },
            auth=None,
            verify=True,
            cert=None,
        )
        requests_mock.reset_mock()

        # basic auth
        connector.config = basic_auth_config
        connector.acquire_data(platform="identitynow")
        requests_mock.assert_called_once_with(
            url="https://example.com/api/v1/things?query=true",
            headers={
                "Content-Type": "application/json",
            },
            auth=("username", "password"),
            verify=False,
            cert=None,
        )
        requests_mock.reset_mock()

        # api key auth
        connector.config = bearer_auth_config
        connector.acquire_data(platform="identitynow")
        requests_mock.assert_called_once_with(
            url="https://example.com/api/v1/things?query=true",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer bearer_token",
            },
            auth=None,
            verify=True,
            cert=None,
        )


def test_get_principals():
    config = HttpConnectorConfig()
    connector = HttpConnector()
    connector.config = config
    connector.source_data = [
        {
            "authoritative": False,
            "systemAccount": False,
            "uncorrelated": False,
            "features": "SYNC_PROVISIONING, DIRECT_PERMISSIONS, PROVISIONING, SEARCH, ENABLE",
            "cloudLifecycleState": None,
            "identityState": "ACTIVE",
            "connectionType": "direct",
            "uuid": None,
            "nativeIdentity": "onyangokariuiki@aol.com.io",
            "description": None,
            "disabled": False,
            "locked": False,
            "type": None,
            "isMachine": False,
            "recommendation": None,
            "manuallyCorrelated": False,
            "hasEntitlements": True,
            "sourceId": "0e7e7f808c484ae1aa7eeb3ba69b284d",
            "sourceName": "Abcd",
            "identityId": "b0094f0086474f059924709ed53ef2e4",
            "identity": {
                "type": "IDENTITY",
                "id": "b0094f0086474f059924709ed53ef2e4",
                "name": "Onyango Kariuiki",
            },
            "sourceOwner": {
                "type": "IDENTITY",
                "id": "2c918088766ca9a30176733d529a3b62",
                "name": "abcd_admin",
            },
            "attributes": {
                "loginID": "onyangokariuiki",
                "displayName": "Onyango Kariuiki",
                "familyName": "kariuiki",
                "givenName": "onyango",
                "active": "true",
                "externalId": None,
                "userName": "onyangokariuiki@aol.com.io",
                "email": "onyangokariuiki@aol.com.io",
                "idNowDescription": "563ae98f3e2833bdd8e7e2ebd1120e3b616c26fd740af31b9beb7105f2afd086",
                "group": ["ReadAllNonSensitive CH1", "ADGROUP::ABCD_All_Users"],
                "urn:ietf:params:scim:nbnExtended.username": "onyangokariuiki",
            },
        }
    ]

    principals: list[PrincipalDio] = connector.get_principals()
    assert principals == [
        # TODO add the rest of the params from PrincipalDbo to PrincipalDio
        PrincipalDio(
            first_name="onyango",
            last_name="kariuiki",
            user_name="onyangokariuiki",
            email="onyangokariuiki@aol.com.io",
        )
    ]
