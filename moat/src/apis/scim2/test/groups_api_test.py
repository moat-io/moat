import json
from flask.testing import FlaskClient
from unittest.mock import patch

from database import Database
from models import PrincipalGroupDbo


def test_auth_failure(flask_test_client: FlaskClient):
    response = flask_test_client.get(
        "/api/scim/v2/Groups",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert response.status_code == 401
    assert response.json == {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
        "detail": f"Authentication failed",
        "status": "401",
    }


def test_create_group(flask_test_client: FlaskClient, database_empty: Database):
    scim_payload = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
        "displayName": "APP_DEVELOPERS_GL",
        "members": [
            {
                "value": "alice.cooper@moat.io",
                "type": "User",
                "$ref": "",
                "display": "",
            },
            {
                "value": "bob.hope@moat.io",
                "type": "User",
                "$ref": "",
                "display": "",
            },
        ],
    }

    with patch(
        "apis.scim2.src.groups_api.create_id",
        return_value="12345678-1234-5678-1234-567812345678",
    ):
        response = flask_test_client.post(
            "/api/scim/v2/Groups",
            data=json.dumps(scim_payload),
            content_type="application/json",
            headers={"Authorization": "Bearer scim-token"},
        )

        assert response.status_code == 201
        assert response.headers["Content-Type"] == "application/scim+json"
        response_payload: dict = response.get_json()

        # we should get the same content back with the id
        assert response_payload == scim_payload | {
            "id": "12345678-1234-5678-1234-567812345678"
        }

        with database_empty.Session.begin() as session:
            principal_group: PrincipalGroupDbo | None = (
                session.query(PrincipalGroupDbo)
                .filter(
                    PrincipalGroupDbo.source_uid
                    == "12345678-1234-5678-1234-567812345678"
                )
                .first()
            )
            assert principal_group is not None

            assert principal_group.fq_name == "APP_DEVELOPERS_GL"
            assert sorted([g.member_fq_name for g in principal_group.members]) == [
                "alice.cooper",
                "bob.hope",
            ]

            assert principal_group.source_uid == "12345678-1234-5678-1234-567812345678"
            assert principal_group.source_type == "scim"
            assert principal_group.scim_payload == scim_payload | {
                "id": "12345678-1234-5678-1234-567812345678"
            }


def test_create_group_with_confliction(
    flask_test_client: FlaskClient, database_empty: Database
):
    with patch(
        "apis.scim2.src.groups_api.create_id",
        return_value="12345678-1234-5678-1234-567812345678",
    ):
        response = flask_test_client.post(
            "/api/scim/v2/Groups",
            data=json.dumps({}),
            content_type="application/json",
            headers={"Authorization": "Bearer scim-token"},
        )
        assert response.status_code == 409
        assert response.headers["Content-Type"] == "application/scim+json"
        assert response.json == {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
            "detail": "Group with ID 12345678-1234-5678-1234-567812345678 already exists",
            "status": 409,
        }


def test_get_groups(flask_test_client: FlaskClient):
    response = flask_test_client.get(
        f"/api/scim/v2/Groups",
        headers={"Authorization": "Bearer scim-token"},
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/scim+json"

    assert response.json == {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
        "totalResults": 1,
        "startIndex": 1,
        "itemsPerPage": 10000,
        "Resources": [
            {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
                "id": "12345678-1234-5678-1234-567812345678",
                "displayName": "APP_DEVELOPERS_GL",
                "members": [
                    {
                        "value": "alice.cooper@moat.io",
                        "type": "User",
                        "$ref": "",
                        "display": "",
                    },
                    {
                        "value": "bob.hope@moat.io",
                        "type": "User",
                        "$ref": "",
                        "display": "",
                    },
                ],
            }
        ],
    }


def test_get_group(flask_test_client: FlaskClient):
    group_id: str = "12345678-1234-5678-1234-567812345678"
    response = flask_test_client.get(
        f"/api/scim/v2/Groups/{group_id}",
        headers={"Authorization": "Bearer scim-token"},
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/scim+json"
    assert response.json == {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
        "id": "12345678-1234-5678-1234-567812345678",
        "displayName": "APP_DEVELOPERS_GL",
        "members": [
            {
                "value": "alice.cooper@moat.io",
                "type": "User",
                "$ref": "",
                "display": "",
            },
            {
                "value": "bob.hope@moat.io",
                "type": "User",
                "$ref": "",
                "display": "",
            },
        ],
    }


def test_get_group_not_found(flask_test_client: FlaskClient):
    group_id: str = "c0835e00-1859-4927-8293-e2fd064fda0a"
    response = flask_test_client.get(
        f"/api/scim/v2/Groups/{group_id}",
        headers={"Authorization": "Bearer scim-token"},
    )

    assert response.status_code == 404
    assert response.headers["Content-Type"] == "application/scim+json"
    assert response.json == {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
        "detail": f"Group with ID c0835e00-1859-4927-8293-e2fd064fda0a not found",
        "status": 404,
    }


def test_update_group(flask_test_client: FlaskClient, database_empty: Database):
    group_id: str = "12345678-1234-5678-1234-567812345678"
    response = flask_test_client.get(
        f"/api/scim/v2/Groups/{group_id}",
        headers={"Authorization": "Bearer scim-token"},
    )
    assert response.status_code == 200

    # change the group data and PUT it back
    # check the name can be changed as well as members
    scim_payload = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
        "id": "12345678-1234-5678-1234-567812345678",
        "displayName": "SOME_OTHER_GROUP_GL",
        "members": [
            {
                "value": "frank.zappa@moat.io",
                "type": "User",
                "$ref": "",
                "display": "",
            },
            {
                "value": "bob.hope@moat.io",
                "type": "User",
                "$ref": "",
                "display": "",
            },
            {
                "value": "boris.yeltsin@moat.io",
                "type": "User",
                "$ref": "",
                "display": "",
            },
        ],
    }

    response = flask_test_client.put(
        f"/api/scim/v2/Groups/{group_id}",
        data=json.dumps(scim_payload),
        content_type="application/json",
        headers={"Authorization": "Bearer scim-token"},
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/scim+json"
    assert response.json == scim_payload

    with database_empty.Session.begin() as session:
        principal_group: PrincipalGroupDbo | None = (
            session.query(PrincipalGroupDbo)
            .filter(
                PrincipalGroupDbo.source_uid == "12345678-1234-5678-1234-567812345678"
            )
            .first()
        )
        assert principal_group is not None

        assert principal_group.fq_name == "SOME_OTHER_GROUP_GL"
        assert sorted([g.member_fq_name for g in principal_group.members]) == [
            "bob.hope",
            "boris.yeltsin",
            "frank.zappa",
        ]

        assert principal_group.source_uid == "12345678-1234-5678-1234-567812345678"
        assert principal_group.source_type == "scim"
        assert principal_group.scim_payload == scim_payload


def test_update_group_not_found(flask_test_client: FlaskClient):
    group_id: str = "c0835e00-1859-4927-8293-e2fd064fda0a"
    response = flask_test_client.put(
        f"/api/scim/v2/Groups/{group_id}",
        data=json.dumps({}),
        content_type="application/json",
        headers={"Authorization": "Bearer scim-token"},
    )

    assert response.status_code == 404
    assert response.headers["Content-Type"] == "application/scim+json"
    assert response.json == {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
        "detail": f"Group with ID c0835e00-1859-4927-8293-e2fd064fda0a not found",
        "status": 404,
    }


def test_delete_group(flask_test_client: FlaskClient):
    group_id: str = "12345678-1234-5678-1234-567812345678"
    response = flask_test_client.delete(
        f"/api/scim/v2/Groups/{group_id}",
        headers={"Authorization": "Bearer scim-token"},
    )

    assert response.status_code == 204
    assert response.headers["Content-Type"] == "application/scim+json"


def test_delete_group_not_found(flask_test_client: FlaskClient):
    group_id: str = "c0835e00-1859-4927-8293-e2fd064fda0a"
    response = flask_test_client.delete(
        f"/api/scim/v2/Groups/{group_id}",
        headers={"Authorization": "Bearer scim-token"},
    )

    assert response.status_code == 404
    assert response.headers["Content-Type"] == "application/scim+json"
    assert response.json == {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:Error"],
        "detail": f"Group with ID c0835e00-1859-4927-8293-e2fd064fda0a not found",
        "status": 404,
    }
