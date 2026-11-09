import re
from datetime import datetime, timedelta, timezone

from flask.testing import FlaskClient
from database import Database
from models import PolicyEvaluatorResultDbo


def test_list_acls_filters_orders_and_paginates(
    flask_test_client: FlaskClient, database_empty: Database
) -> None:
    now = datetime(2026, 11, 9, tzinfo=timezone.utc)
    with database_empty.Session.begin() as session:
        rows = [
            PolicyEvaluatorResultDbo(
                platform="acl-api-platform",
                principal_fq_name="test/alice",
                resource_fq_name="catalog.sales.customers",
                action="FilterTables",
                active=True,
                record_created_date=now,
            ),
            PolicyEvaluatorResultDbo(
                platform="acl-api-platform",
                principal_fq_name="test/bob",
                resource_fq_name="catalog.sales.orders",
                action="FilterTables",
                active=False,
                record_created_date=now - timedelta(days=2),
            ),
            PolicyEvaluatorResultDbo(
                platform="other-platform",
                principal_fq_name="test/charlie",
                resource_fq_name="catalog.sales.customers_archive",
                action="FilterTables",
                active=True,
                record_created_date=now,
            ),
        ]
        session.add_all(rows)
        session.flush()
        row_ids: list[int] = [row.id for row in rows]

    granted_after = (now - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    response = flask_test_client.get(
        "/api/acls/v1"
        f"?platform=acl-api-platform&resource_prefix=catalog.sales.cust"
        f"&granted_after={granted_after}&sort_by=principal&sort_order=asc"
        "&page=1&page_size=1",
        headers={"Authorization": "Bearer bearer-token"},
    )

    assert response.status_code == 200
    assert response.json["page"] == 1
    assert response.json["page_size"] == 1
    assert response.json["total"] == 1
    result = response.json["results"][0]
    assert {
        key: result[key] for key in ("platform", "principal", "resource", "access")
    } == {
        "platform": "acl-api-platform",
        "principal": "test/alice",
        "resource": "catalog.sales.customers",
        "access": True,
    }
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+0000",
        result["granted_at"],
    )

    second_page = flask_test_client.get(
        "/api/acls/v1?platform=acl-api-platform&sort_by=principal"
        "&sort_order=asc&page=2&page_size=1",
        headers={"Authorization": "Bearer bearer-token"},
    )
    assert second_page.status_code == 200
    assert second_page.json["total"] == 2
    assert second_page.json["results"][0]["principal"] == "test/bob"

    ordered = flask_test_client.get(
        "/api/acls/v1?platform=acl-api-platform&sort_by=resource&sort_order=asc",
        headers={"Authorization": "Bearer bearer-token"},
    )
    assert [row["resource"] for row in ordered.json["results"]] == [
        "catalog.sales.customers",
        "catalog.sales.orders",
    ]

    with database_empty.Session.begin() as session:
        session.query(PolicyEvaluatorResultDbo).filter(
            PolicyEvaluatorResultDbo.id.in_(row_ids)
        ).delete(synchronize_session=False)


def test_list_acls_uses_configured_default_page_size(
    flask_test_client: FlaskClient,
) -> None:
    from apis.acls.src.acls_api import api_config

    response = flask_test_client.get(
        "/api/acls/v1", headers={"Authorization": "Bearer bearer-token"}
    )

    assert response.status_code == 200
    assert response.json["page_size"] == api_config.default_page_size


def test_list_acls_validates_pagination_and_ordering(
    flask_test_client: FlaskClient,
) -> None:
    from apis.acls.src.acls_api import api_config

    for query in (
        "page=0",
        "page_size=0",
        f"page_size={api_config.max_page_size + 1}",
        "sort_by=unexpected",
        "sort_order=sideways",
        "granted_after=not-a-date",
    ):
        response = flask_test_client.get(
            f"/api/acls/v1?{query}",
            headers={"Authorization": "Bearer bearer-token"},
        )
        assert response.status_code in {400, 422}, query


def test_list_acls_openapi_documents_filters(flask_test_client: FlaskClient) -> None:
    response = flask_test_client.get(
        "/openapi.json", headers={"Authorization": "Bearer bearer-token"}
    )

    assert response.status_code == 200
    operation = response.json["paths"]["/api/acls/v1"]["get"]
    query_parameters = {
        parameter["name"]
        for parameter in operation["parameters"]
        if parameter["in"] == "query"
    }
    assert query_parameters == {
        "page",
        "page_size",
        "sort_by",
        "sort_order",
        "platform",
        "resource_prefix",
        "granted_after",
    }
