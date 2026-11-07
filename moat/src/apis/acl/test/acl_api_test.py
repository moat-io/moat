from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from flask.testing import FlaskClient

GRANT = SimpleNamespace(
    principal_fq_name="test::principal::admin",
    resource_fq_name="trino.sales.orders",
    platform="trino",
    action="select",
    active=True,
    record_updated_date=datetime(2026, 11, 7, 1, 2, 3, tzinfo=timezone.utc),
)


def test_list_grants_invalid_pagination(flask_test_client: FlaskClient):
    for query in ("page=0", "page_size=0", "page_size=101"):
        response = flask_test_client.get(f"/api/acl/v1/grants?{query}")
        assert response.status_code == 422


def test_list_grants_returns_a_page_of_grants(
    flask_test_client: FlaskClient, monkeypatch: pytest.MonkeyPatch
):
    def _mock_get_grants_paginated(**kwargs):
        assert kwargs["page_number"] == 0
        assert kwargs["page_size"] == 20
        assert kwargs["search_term"] == ""
        assert kwargs["active"] is True
        return 1, [GRANT]

    monkeypatch.setattr(
        "apis.acl.src.acl_api.PolicyEvaluatorResultRepository.get_grants_paginated",
        _mock_get_grants_paginated,
    )

    response = flask_test_client.get("/api/acl/v1/grants")

    assert response.status_code == 200
    assert response.json == {
        "page": 1,
        "page_size": 20,
        "total": 1,
        "results": [
            {
                "principal_fq_name": "test::principal::admin",
                "resource_fq_name": "trino.sales.orders",
                "platform": "trino",
                "action": "select",
                "active": True,
                "record_updated_date": "2026-11-07T01:02:03+00:00",
            }
        ],
    }


def test_list_grants_passes_the_query_through(
    flask_test_client: FlaskClient, monkeypatch: pytest.MonkeyPatch
):
    captured: dict = {}

    def _mock_get_grants_paginated(**kwargs):
        captured.update(kwargs)
        return 0, []

    monkeypatch.setattr(
        "apis.acl.src.acl_api.PolicyEvaluatorResultRepository.get_grants_paginated",
        _mock_get_grants_paginated,
    )

    response = flask_test_client.get(
        "/api/acl/v1/grants"
        "?page=3&page_size=5&search=alice&sort_key=platform&sort_ascending=false&active=false"
    )

    assert response.status_code == 200
    assert captured["page_number"] == 2
    assert captured["page_size"] == 5
    assert captured["search_term"] == "alice"
    assert captured["sort_col_name"] == "platform"
    assert captured["sort_ascending"] is False
    assert captured["active"] is False
