from flask.testing import FlaskClient


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
    assert response.status_code == 200
