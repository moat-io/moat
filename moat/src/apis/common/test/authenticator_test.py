from apis.models import ApiConfig
from apis.common.src.authenticator import authenticate
import pytest
from unittest.mock import patch
from werkzeug.exceptions import Unauthorized
import jwt
import datetime


def test_authenticator():
    config: ApiConfig = ApiConfig()
    config.auth_method = "none"
    assert authenticate(config).__name__ == "no_auth_decorator"

    config.auth_method = "api-key"
    assert authenticate(config).__name__ == "api_key_auth_decorator"

    config.auth_method = "oauth2"
    assert authenticate(config).__name__ == "oauth2_auth_decorator"


def test_authenticator_invalid_method(caplog: pytest.LogCaptureFixture):
    config: ApiConfig = ApiConfig()
    config.auth_method = "invalid"
    with pytest.raises(ValueError):
        authenticate(config)
    assert "Invalid auth method: invalid" in caplog.text


def test_authenticator_none_method(caplog: pytest.LogCaptureFixture):
    config: ApiConfig = ApiConfig()
    config.auth_method = "none"
    decorator = authenticate(config)

    def test_func():
        return "test"

    assert decorator(test_func)() == "test"
    assert "Processed api call without authentication" in caplog.text


def test_authenticator_api_key_method(caplog: pytest.LogCaptureFixture):
    config: ApiConfig = ApiConfig()
    config.auth_method = "api-key"
    config.api_key = "bearer-token"
    decorator = authenticate(config)

    def test_func():
        return "test"

    with patch(
        "apis.common.src.authenticator._get_bearer_token",
        return_value="bearer-token",
    ):
        assert decorator(test_func)() == "test"
        assert "with api-key" in caplog.text

    # negative test
    with patch(
        "apis.common.src.authenticator._get_bearer_token",
        return_value="bad-bearer-token",
    ):
        with pytest.raises(Unauthorized):
            decorator(test_func)()
        assert "with api-key" in caplog.text


def _create_rs256_keypair():
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    public_key = private_key.public_key()

    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    public_key_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private_key_pem, public_key_pem


def validate_token(token_content: dict, caplog: pytest.LogCaptureFixture):
    config: ApiConfig = ApiConfig()
    config.auth_method = "oauth2"
    config.oauth2_client_id = "client-id"
    config.oauth2_client_secret = "client-secret"
    config.oauth2_issuer_url = "issuer-url"
    config.oauth2_audience = "account"
    config.oauth2_read_scope = "scim-read"
    config.oauth2_write_scope = "scim-write"

    private_key_pem, public_key_pem = _create_rs256_keypair()
    access_token: str = jwt.encode(token_content, private_key_pem, algorithm="RS256")

    def test_func():
        return "test"

    with patch(
        "apis.common.src.authenticator._get_bearer_token", return_value=access_token
    ):
        with patch(
            "apis.common.src.authenticator._get_oauth2_signing_key",
            return_value=public_key_pem,
        ):
            decorator = authenticate(api_config=config, scope="scim-read")
            assert decorator(test_func)() == "test"
            assert (
                "Authenticated request with required oauth2 scope scim-read"
                in caplog.text
            )


def test_authenticator_oauth2_method_valid(caplog: pytest.LogCaptureFixture):
    token_content: dict = {
        "exp": (datetime.datetime.now() + datetime.timedelta(minutes=15)).timestamp(),
        "iat": (datetime.datetime.now() - datetime.timedelta(minutes=15)).timestamp(),
        "jti": "cbe7eb82-5276-4921-ac23-35999ef0eceb",
        "iss": "http://localhost:8080/realms/moat",
        "aud": "account",
        "sub": "d895b729-38df-462c-bd74-8d9ec81ef81a",
        "typ": "Bearer",
        "azp": "moat-api-client",
        "acr": "1",
        "allowed-origins": ["/*"],
        "realm_access": {
            "roles": ["offline_access", "default-roles-moat", "uma_authorization"]
        },
        "resource_access": {
            "account": {
                "roles": ["manage-account", "manage-account-links", "view-profile"]
            }
        },
        "scope": "profile scim-read scim-write email",
        "clientHost": "10.89.1.5",
        "email_verified": False,
        "preferred_username": "service-account-moat-api-client",
        "clientAddress": "10.89.1.5",
        "client_id": "moat-api-client",
    }
    validate_token(token_content, caplog)


def test_authenticator_oauth2_method_expired(caplog: pytest.LogCaptureFixture):
    token_content: dict = {
        "exp": (datetime.datetime.now() - datetime.timedelta(minutes=10)).timestamp(),
        "iat": (datetime.datetime.now() - datetime.timedelta(minutes=15)).timestamp(),
        "jti": "cbe7eb82-5276-4921-ac23-35999ef0eceb",
        "iss": "http://localhost:8080/realms/moat",
        "aud": "account",
        "sub": "d895b729-38df-462c-bd74-8d9ec81ef81a",
        "typ": "Bearer",
        "azp": "moat-api-client",
        "acr": "1",
        "allowed-origins": ["/*"],
        "realm_access": {
            "roles": ["offline_access", "default-roles-moat", "uma_authorization"]
        },
        "resource_access": {
            "account": {
                "roles": ["manage-account", "manage-account-links", "view-profile"]
            }
        },
        "scope": "profile scim-read scim-write email",
        "clientHost": "10.89.1.5",
        "email_verified": False,
        "preferred_username": "service-account-moat-api-client",
        "clientAddress": "10.89.1.5",
        "client_id": "moat-api-client",
    }
    with pytest.raises(Unauthorized):
        validate_token(token_content, caplog)

    assert "Failed to decode token: Signature has expired" in caplog.text


def test_authenticator_oauth2_method_audience_bad(caplog: pytest.LogCaptureFixture):
    token_content: dict = {
        "exp": (datetime.datetime.now() + datetime.timedelta(minutes=15)).timestamp(),
        "iat": (datetime.datetime.now() - datetime.timedelta(minutes=15)).timestamp(),
        "jti": "cbe7eb82-5276-4921-ac23-35999ef0eceb",
        "iss": "http://localhost:8080/realms/moat",
        "aud": "bad-audience",
        "sub": "d895b729-38df-462c-bd74-8d9ec81ef81a",
        "typ": "Bearer",
        "azp": "moat-api-client",
        "acr": "1",
        "allowed-origins": ["/*"],
        "realm_access": {
            "roles": ["offline_access", "default-roles-moat", "uma_authorization"]
        },
        "resource_access": {
            "account": {
                "roles": ["manage-account", "manage-account-links", "view-profile"]
            }
        },
        "scope": "profile scim-read scim-write email",
        "clientHost": "10.89.1.5",
        "email_verified": False,
        "preferred_username": "service-account-moat-api-client",
        "clientAddress": "10.89.1.5",
        "client_id": "moat-api-client",
    }
    with pytest.raises(Unauthorized):
        validate_token(token_content, caplog)

    assert "Failed to decode token: Audience doesn't match" in caplog.text


def test_authenticator_oauth2_method_missing_scope(caplog: pytest.LogCaptureFixture):
    token_content: dict = {
        "exp": (datetime.datetime.now() + datetime.timedelta(minutes=15)).timestamp(),
        "iat": (datetime.datetime.now() - datetime.timedelta(minutes=15)).timestamp(),
        "jti": "cbe7eb82-5276-4921-ac23-35999ef0eceb",
        "iss": "http://localhost:8080/realms/moat",
        "aud": "account",
        "sub": "d895b729-38df-462c-bd74-8d9ec81ef81a",
        "typ": "Bearer",
        "azp": "moat-api-client",
        "acr": "1",
        "allowed-origins": ["/*"],
        "realm_access": {
            "roles": ["offline_access", "default-roles-moat", "uma_authorization"]
        },
        "resource_access": {
            "account": {
                "roles": ["manage-account", "manage-account-links", "view-profile"]
            }
        },
        "scope": "profile scim-write email",
        "clientHost": "10.89.1.5",
        "email_verified": False,
        "preferred_username": "service-account-moat-api-client",
        "clientAddress": "10.89.1.5",
        "client_id": "moat-api-client",
    }
    with pytest.raises(Unauthorized):
        validate_token(token_content, caplog)

    assert "Unauthorized request (missing scope scim-read) with token" in caplog.text
