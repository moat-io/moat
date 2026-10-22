from types import SimpleNamespace

import pytest


from flask.testing import FlaskClient


def test_list_user_entitlements_invalid_pagination(flask_test_client: FlaskClient):
    for query in ("page=0", "page_size=0", "page_size=101"):
        response = flask_test_client.get(f"/api/entitlements/v1/users?{query}")
        assert response.status_code == 400


def test_list_user_entitlements_serializes_string_entitlements(
    flask_test_client: FlaskClient, monkeypatch: pytest.MonkeyPatch
):
    principal = SimpleNamespace(
        user_name="admin",
        fq_name="test::principal::admin",
        attributes=["admin_access", "system_configuration"],
    )

    def _mock_get_all_with_search_and_pagination(**kwargs):
        assert kwargs["page_number"] == 0
        assert kwargs["page_size"] == 20
        assert kwargs["search_term"] == ""
        assert kwargs["source_type"] is None
        return 1, [principal]

    monkeypatch.setattr(
        "apis.entitlements.src.user_entitlements.PrincipalRepository.get_all_with_search_and_pagination",
        _mock_get_all_with_search_and_pagination,
    )

    response = flask_test_client.get("/api/entitlements/v1/users")

    assert response.status_code == 200
    assert response.json == {
        "page": 1,
        "page_size": 20,
        "total": 1,
        "results": [
            {
                "username": "admin",
                "fq_name": "test::principal::admin",
                "resources": [
                    {"attribute_value": "admin_access", "attribute_key": None},
                    {
                        "attribute_value": "system_configuration",
                        "attribute_key": None,
                    },
                ],
            }
        ],
    }


def test_list_user_entitlements_serializes_attribute_entitlements(
    flask_test_client: FlaskClient, monkeypatch: pytest.MonkeyPatch
):
    entitlement = SimpleNamespace(
        attribute_key="role",
        attribute_value="user_management",
    )
    principal = SimpleNamespace(
        user_name="admin",
        fq_name="test::principal::admin",
        attributes=[entitlement],
    )

    def _mock_get_all_with_search_and_pagination(**kwargs):
        assert kwargs["page_number"] == 1
        assert kwargs["page_size"] == 10
        assert kwargs["search_term"] == "adm"
        assert kwargs["source_type"] == "scim"
        return 1, [principal]

    monkeypatch.setattr(
        "apis.entitlements.src.user_entitlements.PrincipalRepository.get_all_with_search_and_pagination",
        _mock_get_all_with_search_and_pagination,
    )

    response = flask_test_client.get(
        "/api/entitlements/v1/users?page=2&page_size=10&search=adm&source=scim"
    )

    assert response.status_code == 200
    assert response.json == {
        "page": 2,
        "page_size": 10,
        "total": 1,
        "results": [
            {
                "username": "admin",
                "fq_name": "test::principal::admin",
                "resources": [
                    {"attribute_value": "user_management", "attribute_key": "role"}
                ],
            }
        ],
    }


def test_list_user_entitlements_openapi_documents_query_params(
    flask_test_client: FlaskClient,
):
    response = flask_test_client.get("/openapi.json")

    assert response.status_code == 200
    operation = response.json["paths"]["/api/entitlements/v1/users"]["get"]
    parameter_names = {parameter["name"] for parameter in operation["parameters"]}

    assert operation["tags"] == ["mcp"]
    assert parameter_names == {"page", "page_size", "search", "source", "format"}
