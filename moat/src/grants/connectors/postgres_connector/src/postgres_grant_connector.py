import psycopg
from dataclasses import dataclass
from .postgres_grant_connector_config import PostgresGrantConnectorConfig
from ...connector_base import GrantConnectorBase


@dataclass(frozen=True)
class PostgresGrant:
    username: str
    database_name: str
    schema_name: str
    table_name: str
    type: str


class GenericGrant:
    username: str
    resource_name: str
    resource_type: str
    grant_type: str


class PostgresGrantConnector(GrantConnectorBase):

    statements: list[str] = []

    def __init__(self):
        super().__init__()
        self.config: PostgresGrantConnectorConfig = self.get_config()
        self.connection: psycopg.Connection = self.connect(config=self.config)

    def connect(self, config: PostgresGrantConnectorConfig) -> psycopg.Connection:
        # TODO enable SSL
        return psycopg.connect(
            host=config.host,
            port=config.port,
            user=config.username,
            password=config.password,
            dbname=config.database,
        )

    def get_config(self) -> PostgresGrantConnectorConfig:
        return PostgresGrantConnectorConfig.load()

    def disconnect(self, connection: psycopg.Connection):
        connection.close()

    def add_statement(self, statement: str):
        self.statements.append(statement)

    def _list_existing_principals(self) -> set[str]:
        with self.connection.cursor() as cursor:
            result = cursor.execute(self.config.principals_query)
            usernames: set[str] = set([r[0] for r in result])
            return usernames

    def _list_existing_grants(self, cursor: psycopg.cursor) -> set[PostgresGrant]:
        # TODO move to config
        cursor.execute(
            f"""SELECT grantee as username,
                       table_catalog as database_name,
                       table_schema as schema_name,
                       table_name,
                       privilege_type as type
                FROM information_schema.role_table_grants
                WHERE table_catalog = 'warehouse'
                and table_schema in ('sales', 'marketing')
                and privilege_type = 'INSERT'
            """
        )
        return {
            PostgresGrant(
                username=r[0],
                database_name=r[1],
                schema_name=r[2],
                table_name=r[3],
                type=r[4],
            )
            for r in cursor.fetchall()
        }

    def _list_required_grants(self) -> set[PostgresGrant]:
        """
        This should be in the base class as it is not postgres specific
        Loop on users, for each user, loop on tables, for each table, check if user has grant on table
        """
        return set()

    def sync_principals(self, active_usernames: set[str]):
        target_usernames: set[str] = self._list_existing_principals()
        usernames_to_add: set[str] = active_usernames - target_usernames

        for username in usernames_to_add:
            self.add_statement(f"create user {username};")

    def sync_grants(self, active_usernames: set[str]) -> None:
        with self.connection.cursor() as cursor:
            existing_grants: set[PostgresGrant] = self._list_existing_grants(
                cursor=cursor
            )

        required_grants: set[PostgresGrant] = self._list_required_grants()

        grants_to_add: set[PostgresGrant] = required_grants - existing_grants
        grants_to_revoke: set[PostgresGrant] = existing_grants - required_grants

        for grant in grants_to_add:
            # only add grants to in-scope active users
            if grant.username in active_usernames:
                self.add_statement(
                    f"grant connect on database {grant.database_name} to {grant.username};"
                )
                self.add_statement(
                    f"grant usage on schema {grant.schema_name} to {grant.username};"
                )
                self.add_statement(
                    f"grant select on {grant.schema_name}.{grant.table_name} to {grant.username};"
                )

        for grant in grants_to_revoke:
            # only add grants to in-scope active/inactive users
            if grant.username in active_usernames:
                self.add_statement(
                    f"revoke all on {grant.schema_name}.{grant.table_name} from {grant.username}"
                )

    def execute_statements(self):
        with self.connection.cursor() as cursor:
            for statement in self.statements:
                cursor.execute(statement)
        self.connection.commit()
