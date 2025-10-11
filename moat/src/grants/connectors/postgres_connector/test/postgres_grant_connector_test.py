from unittest import mock
from database import Database
from sqlalchemy.sql import text
from ..src.postgres_grant_connector import PostgresGrantConnector, PostgresGrant


# with database_warehouse.Session.begin() as session:
#     # check users exist
#     result = session.execute(
#         text("select rolname from pg_roles where rolname not like 'pg_%'")
#     ).all()
#
#     usernames: list[str] = [r[0] for r in result]
#
#     for actual in ["admin", "bob", "alice", "frank", "anne"]:
#         assert actual in usernames
#
#     # check user table access for alice
#     # warehouse.sales.customer_markets - allowed
#     # warehouse.sales.customers - not allowed
#     result = session.execute(
#         text(
#             "select table_name from information_schema.role_table_grants where grantee = 'alice';"
#         )
#     ).all()
#
#     assert len(result) == 1
#     assert result[0][0] == "customer_markets"


def mock_get_grants(username: str, table: str) -> bool:
    return username == "alice" and table == "warehouse.sales.customer_markets"


def test_sync_grants__fresh_db(database: Database, database_warehouse: Database):
    connector = PostgresGrantConnector()

    mock_list_required_grants: set[PostgresGrant] = {
        PostgresGrant(
            username="admin",
            database_name="warehouse",
            schema_name="sales",
            table_name="customer_markets",
            type="SELECT",
        ),
        PostgresGrant(
            username="bob",
            database_name="warehouse",
            schema_name="sales",
            table_name="customer_markets",
            type="SELECT",
        ),
    }

    with mock.patch.object(
        PostgresGrantConnector,
        "_list_required_grants",
        return_value=mock_list_required_grants,
    ):
        connector.sync(database=database, platform="warehouse")
        connector.execute_statements()

    # check the right SQL statements have been created
    assert sorted(connector.statements) == sorted(
        [
            "create user admin;",
            "create user bob;",
            "create user alice;",
            "create user frank;",
            "create user anne;",
            "grant connect on database warehouse to admin;",
            "grant connect on database warehouse to bob;",
            "grant usage on schema sales to admin;",
            "grant usage on schema sales to bob;",
            "grant select on sales.customer_markets to admin;",
            "grant select on sales.customer_markets to bob;",
        ]
    )

    # add a grant which should be there
    with database_warehouse.Session.begin() as session:
        session.execute(text("grant select on sales.customer_markets to bob;"))
        session.commit()

    with mock.patch.object(
        PostgresGrantConnector,
        "_list_required_grants",
        return_value=mock_list_required_grants,
    ):
        connector.sync(database=database, platform="warehouse")

    # should not need a grant on customer_markets for bob
    assert sorted(connector.statements) == sorted(
        [
            "create user admin;",
            "create user bob;",
            "create user alice;",
            "create user frank;",
            "create user anne;",
            "grant connect on database warehouse to admin;",
            "grant connect on database warehouse to bob;",
            "grant usage on schema sales to admin;",
            "grant usage on schema sales to bob;",
            "grant select on sales.customer_markets to admin;",
        ]
    )
