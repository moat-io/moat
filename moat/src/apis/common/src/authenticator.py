from flask import request, abort
from app_logger import Logger, get_logger
from apis.models import ApiConfig
import jwt

logger: Logger = get_logger("authenticator")


def _get_bearer_token(http_request):
    try:
        return http_request.headers.get("Authorization").split(" ")[1]
    except AttributeError:
        abort(400, description="Authorization header missing or invalid")


def _get_oauth2_signing_key(access_token: str, jwks_uri: str) -> str:
    jwks_client = jwt.PyJWKClient(uri=jwks_uri)
    signing_key = jwks_client.get_signing_key_from_jwt(access_token)
    return signing_key.key


def authenticate(api_config: ApiConfig, scope: str = None):

    def no_auth_decorator(func):
        def wrapper(*args, **kwargs):
            logger.info(f"Processed api call without authentication")
            return func(*args, **kwargs)

        wrapper.__name__ = func.__name__
        return wrapper

    def api_key_auth_decorator(func):
        def wrapper(*args, **kwargs):
            if _get_bearer_token(request) == api_config.api_key:
                logger.info(f"Authenticated request with api-key")
                return func(*args, **kwargs)

            logger.info(f"Unauthorized request - api-key did not match")
            abort(401, description="Authentication failed")

        wrapper.__name__ = func.__name__
        return wrapper

    def oauth2_auth_decorator(func):
        def wrapper(*args, **kwargs):
            access_token = _get_bearer_token(request)
            key: str = _get_oauth2_signing_key(access_token, api_config.oauth2_jwks_uri)

            try:
                decoded_token: dict = jwt.decode(
                    access_token,
                    key,
                    algorithms=api_config.oauth2_algorithms,
                    issuer=api_config.oauth2_issuer,
                    audience=api_config.oauth2_audience,
                    options={
                        "verify_signature": True,
                        "verify_exp": True,
                        "verify_nbf": True,
                        "verify_iat": True,
                        "verify_aud": True,
                        "verify_iss": True,
                    },
                )
            except jwt.PyJWTError as e:
                logger.info(f"Failed to decode token: {e}")
                abort(401, description="Authentication failed - bad token")

            scope_list: list[str] = decoded_token.get("scope", "").split(" ")

            if scope_list and scope in scope_list:
                logger.info(f"Authenticated request with required oauth2 scope {scope}")
                return func(*args, **kwargs)

            logger.info(
                f"Unauthorized request (missing scope {scope}) with token: {decoded_token}"
            )
            abort(401, description=f"Authentication failed - missing scope {scope}")

        wrapper.__name__ = func.__name__
        return wrapper

    if api_config.auth_method == "none":
        return no_auth_decorator
    if api_config.auth_method == "api-key":
        return api_key_auth_decorator
    if api_config.auth_method == "oauth2":
        return oauth2_auth_decorator
    else:
        message: str = f"Invalid auth method: {api_config.auth_method}"
        logger.info(message)
        raise ValueError(message)
