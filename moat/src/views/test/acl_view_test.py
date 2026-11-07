from database import Database
from flask.testing import FlaskClient
from models import PolicyEvaluatorResultDbo


def test_acl_index_renders(flask_test_client: FlaskClient):
    response = flask_test_client.get("/acl/", headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert b"Access Control List" in response.data


def test_acl_table_renders_the_grants(
    flask_test_client: FlaskClient, database_empty: Database
):
    with database_empty.Session.begin() as session:
        session.query(PolicyEvaluatorResultDbo).delete()
        session.add(
            PolicyEvaluatorResultDbo(
                principal_fq_name="test::principal::alice",
                resource_fq_name="trino.sales.orders",
                platform="trino",
                action="select",
                active=True,
            )
        )

    response = flask_test_client.get("/acl/table")
    assert response.status_code == 200
    assert b"test::principal::alice" in response.data
    assert b"trino.sales.orders" in response.data


def test_acl_table_renders_the_empty_state(
    flask_test_client: FlaskClient, database_empty: Database
):
    with database_empty.Session.begin() as session:
        session.query(PolicyEvaluatorResultDbo).delete()

    response = flask_test_client.get("/acl/table?search_term=nothing-matches-this")
    assert response.status_code == 200
    assert b"No grants match these filters" in response.data
