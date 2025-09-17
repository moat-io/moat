from app_config import AppConfigModelBase


class TrinoClientConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "trino_client"
    host: str = None
    port: int = None
    username: str = None
    password: str = None
    jwt_token: str = None
    http_scheme: str = "https"
    ssl_verify: bool = True
