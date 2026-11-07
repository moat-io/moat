from flask.testing import FlaskClient

from database import Database
from models import PolicyEvaluatorResultDbo


def test_acls_index(flask_test_client: FlaskClient) -> None:
    response = flask_test_client.get(
        "/acls/?search_term=alice", headers={"HX-Request": "true"}
    )

    assert response.status_code == 200
    assert b"Access Control Lists" in response.data
    assert b"Search principal and resource" in response.data
    assert b'value="alice"' in response.data


def test_acls_table_search_and_render(
    flask_test_client: FlaskClient, database_empty: Database
) -> None:
    with database_empty.Session.begin() as session:
        rows = [
            PolicyEvaluatorResultDbo(
                principal_fq_name="acl-view-test/alice",
                resource_fq_name="datalake.sales.customers",
                platform="trino",
                action="FilterTables",
                active=True,
            ),
            PolicyEvaluatorResultDbo(
                principal_fq_name="acl-view-test/bob",
                resource_fq_name="datalake.sales.orders",
                platform="trino",
                action="FilterTables",
                active=False,
            ),
        ]
        session.add_all(rows)
        session.flush()
        row_ids: list[int] = [row.id for row in rows]

    response = flask_test_client.get(
        "/acls/table?search_term=acl-view-test&sort_key=principal_fq_name",
        headers={"HX-Request": "true"},
    )

    assert response.status_code == 200
    assert b"acl-view-test/alice" in response.data
    assert b"acl-view-test/bob" in response.data
    assert b"datalake.sales.customers" in response.data
    assert b"Access granted" in response.data
    assert b"Access revoked" in response.data
    assert response.headers["HX-Push-Url"].startswith("/acls/?")
