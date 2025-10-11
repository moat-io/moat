from ...connector_base import GrantConnectorBase


class RedshiftGrantConnector(GrantConnectorBase):

    def create_user(self, user_name: str):
        f"CREATE USER {user_name};"

    def grant_database_to_user(self, user_name: str, database_name: str):
        "GRANT CONNECT ON DATABASE your_database_name TO new_user;"

    def grant_schema_to_user(self, user_name: str, schema_name: str):
        "GRANT USAGE ON SCHEMA your_schema_name TO new_user;"

    def grant_table_to_user(
        self,
        user_name: str,
        schema_name: str,
        table_name: str,
        all_columns: set[str],
        allowed_columns: set[str],
        masked_columns: set[str],
    ):
        f"GRANT SELECT ({allowed_columns}) ON {schema_name}.{table_name} TO {user_name}"
