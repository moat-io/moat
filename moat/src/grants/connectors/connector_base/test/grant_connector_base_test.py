from unittest import mock
from database import Database
from ..src.grant_connector_base import GrantConnectorBase
from ..src.test_connector import TestConnector

all_principals: dict = {
    n: {"tables": []} for n in ["admin", "alice", "anne", "bob", "frank"]
}

extra_principals: dict = {"hayley": {"tables": []}}


def test_sync_users(database: Database):
    # empty target system
    test_connector: TestConnector = TestConnector()
    test_connector.sync(database=database)

    assert test_connector.principals.keys() == all_principals.keys()

    # partially populated target system
    test_connector: TestConnector = TestConnector()
    test_connector.principals = {"alice": {"tables": []}}
    test_connector.sync(database=database)

    assert test_connector.principals.keys() == all_principals.keys()

    # fully populated target system
    test_connector: TestConnector = TestConnector()
    test_connector.principals = all_principals
    test_connector.sync(database=database)

    assert test_connector.principals.keys() == all_principals.keys()

    # over populated target system - should not delete unknown users
    test_connector: TestConnector = TestConnector()
    test_connector.principals = all_principals | extra_principals
    test_connector.sync(database=database)

    assert (
        test_connector.principals.keys()
        == all_principals.keys() | extra_principals.keys()
    )


def test_sync_table_grants(database: Database):
    # empty target system
    test_connector: TestConnector = TestConnector()

    with mock.patch.object(
        GrantConnectorBase,
        "get_grants",
        side_effect=lambda u, t: u == "alice" and t.startswith("datalake.hr"),
    ):
        test_connector.sync(database=database)
        assert sorted(test_connector.principals["alice"]["tables"]) == [
            "datalake.hr.employee_territories",
            "datalake.hr.employees",
        ]

    # remove one of the grants
    with mock.patch.object(
        GrantConnectorBase,
        "get_grants",
        side_effect=lambda u, t: u == "alice" and t == "datalake.hr.employees",
    ):
        test_connector.sync(database=database)
        assert sorted(test_connector.principals["alice"]["tables"]) == [
            "datalake.hr.employees",
        ]
