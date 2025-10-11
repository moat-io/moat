from app_config import AppConfigModelBase


class PostgresGrantConnectorConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "grant_connector.postgres"
    host: str = None
    port: int = None
    username: str = None
    password: str = None
    database: str = None

    principals_query: str = (
        "select rolname as username from pg_roles where rolname not like 'pg_%'"
    )

    remove_grants_from_objects_regex: str = None
    drop_users: bool = False
    dry_run: bool = False
