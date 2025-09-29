from flask.testing import FlaskClient

from database import Database
from models import OpaBundleDbo


def test_index(flask_test_client: FlaskClient):
    # no path param
    response = flask_test_client.get("/api/v1/opa/bundle/")
    assert response.status_code == 404

    # no auth header
    response = flask_test_client.get("/api/v1/opa/bundle/trino")
    assert response.status_code == 400
    assert response.json == {
        "detail": "Authorization header missing or invalid",
        "status": "400",
    }

    # good auth
    response = flask_test_client.get(
        "/api/v1/opa/bundle/trino", headers={"Authorization": "Bearer bearer-token"}
    )
    assert response.status_code == 404


def test_etag_caching(flask_test_client: FlaskClient, database_empty: Database):
    # no bundle
    response = flask_test_client.get(
        "/api/v1/opa/bundle/trino", headers={"Authorization": "Bearer bearer-token"}
    )
    assert response.status_code == 404

    # create bundle
    with database_empty.Session.begin() as session:
        opa_bundle_dbo: OpaBundleDbo = OpaBundleDbo()
        opa_bundle_dbo.platform = "trino"
        opa_bundle_dbo.bundle_filename = "bundle.tar.gz"
        opa_bundle_dbo.bundle_directory = "apis/opa/test"
        opa_bundle_dbo.e_tag = "553ae7da92f5505a92bbb8c9d47be76ab9f65bc2"
        session.add(opa_bundle_dbo)
        session.commit()

    # no etag header in request
    response = flask_test_client.get(
        "/api/v1/opa/bundle/trino", headers={"Authorization": "Bearer bearer-token"}
    )
    assert response.status_code == 200
    assert response.headers.get("ETag") == "553ae7da92f5505a92bbb8c9d47be76ab9f65bc2"

    # if-none-match header matches
    response = flask_test_client.get(
        "/api/v1/opa/bundle/trino",
        headers={
            "Authorization": "Bearer bearer-token",
            "If-None-Match": "553ae7da92f5505a92bbb8c9d47be76ab9f65bc2",
        },
    )
    assert response.status_code == 304

    # if-none-match header does not match, return 200 with new etag
    response = flask_test_client.get(
        "/api/v1/opa/bundle/trino",
        headers={
            "Authorization": "Bearer bearer-token",
            "If-None-Match": "hash-of-a-bundle-which-doesnt-exist",
        },
    )
    assert response.status_code == 200
    assert response.headers.get("ETag") == "553ae7da92f5505a92bbb8c9d47be76ab9f65bc2"

    assert response.text == "fake tarball"
