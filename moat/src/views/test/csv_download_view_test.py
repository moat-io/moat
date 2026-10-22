import csv
from io import StringIO

from database import Database
from flask.testing import FlaskClient
from models import (
    PrincipalAttributeDbo,
    PrincipalDbo,
    ResourceAttributeDbo,
    ResourceDbo,
)

# the database fixture is module scoped, so rows written by one test are still
# present for the next. Each test below therefore tags its own rows with a unique
# source/attribute value and filters on that tag.


def _csv_rows(response) -> list[dict[str, str]]:
    return list(csv.DictReader(StringIO(response.get_data(as_text=True))))


def _add_principal(
    session, user_name: str, source_type: str, attributes: dict[str, str] = None
) -> None:
    principal: PrincipalDbo = PrincipalDbo()
    principal.fq_name = user_name
    principal.first_name = "Csv"
    principal.last_name = "User"
    principal.user_name = user_name
    principal.email = f"{user_name}@mail.com"
    principal.source_type = source_type
    session.add(principal)

    for attribute_key, attribute_value in (attributes or {}).items():
        attribute: PrincipalAttributeDbo = PrincipalAttributeDbo()
        attribute.fq_name = user_name
        attribute.attribute_key = attribute_key
        attribute.attribute_value = attribute_value
        session.add(attribute)


def _add_resource(
    session,
    fq_name: str,
    platform: str,
    object_type: str,
    attributes: dict[str, str] = None,
) -> None:
    resource: ResourceDbo = ResourceDbo()
    resource.fq_name = fq_name
    resource.platform = platform
    resource.object_type = object_type
    session.add(resource)

    for attribute_key, attribute_value in (attributes or {}).items():
        attribute: ResourceAttributeDbo = ResourceAttributeDbo()
        attribute.fq_name = fq_name
        attribute.attribute_key = attribute_key
        attribute.attribute_value = attribute_value
        session.add(attribute)


def test_download_principals_csv(
    flask_test_client: FlaskClient, database_empty: Database
):
    with database_empty.Session.begin() as session:
        _add_principal(
            session=session,
            user_name="csv.user",
            source_type="ldap",
            attributes={"department": "finance"},
        )
        session.commit()

    response = flask_test_client.get("/principals/download")

    assert response.status_code == 200
    assert response.mimetype == "text/csv"
    assert (
        response.headers["Content-Disposition"] == "attachment; filename=principals.csv"
    )

    lines: list[str] = response.get_data(as_text=True).splitlines()
    assert lines[0].startswith("user_name,first_name,last_name,email,source_type")
    assert "csv.user,Csv,User,csv.user@mail.com,ldap,True,department=finance" in lines


def test_download_resources_csv(
    flask_test_client: FlaskClient, database_empty: Database
):
    with database_empty.Session.begin() as session:
        _add_resource(
            session=session,
            fq_name="datalake.sales.orders",
            platform="trino",
            object_type="table",
        )
        session.commit()

    response = flask_test_client.get("/resources/download")

    assert response.status_code == 200
    assert response.mimetype == "text/csv"
    assert (
        response.headers["Content-Disposition"] == "attachment; filename=resources.csv"
    )

    lines: list[str] = response.get_data(as_text=True).splitlines()
    assert lines[0] == "fq_name,platform,object_type,active,attributes"
    assert "datalake.sales.orders,trino,table,True," in lines


def test_download_principals_csv_applies_source_filter(
    flask_test_client: FlaskClient, database_empty: Database
):
    with database_empty.Session.begin() as session:
        _add_principal(session=session, user_name="src.ldap", source_type="src-ldap")
        _add_principal(session=session, user_name="src.scim", source_type="src-scim")
        session.commit()

    response = flask_test_client.get("/principals/download?source_type=src-scim")

    assert {row["user_name"] for row in _csv_rows(response)} == {"src.scim"}


def test_download_principals_csv_applies_attribute_filter(
    flask_test_client: FlaskClient, database_empty: Database
):
    with database_empty.Session.begin() as session:
        _add_principal(
            session=session,
            user_name="attr.finance",
            source_type="attr-test",
            attributes={"attr_department": "attr-finance"},
        )
        _add_principal(
            session=session,
            user_name="attr.sales",
            source_type="attr-test",
            attributes={"attr_department": "attr-sales"},
        )
        session.commit()

    response = flask_test_client.get(
        "/principals/download?attributes=attr_department:attr-sales"
    )

    assert {row["user_name"] for row in _csv_rows(response)} == {"attr.sales"}


def test_download_principals_csv_applies_search_term(
    flask_test_client: FlaskClient, database_empty: Database
):
    with database_empty.Session.begin() as session:
        _add_principal(
            session=session, user_name="searchable.alice", source_type="search-test"
        )
        _add_principal(
            session=session, user_name="searchable.bob", source_type="search-test"
        )
        session.commit()

    response = flask_test_client.get(
        "/principals/download?source_type=search-test&search_term=alic"
    )

    assert {row["user_name"] for row in _csv_rows(response)} == {"searchable.alice"}


def test_download_principals_csv_is_not_paginated(
    flask_test_client: FlaskClient, database_empty: Database
):
    """The download is the whole result set, not the page currently on screen."""
    with database_empty.Session.begin() as session:
        for index in range(25):
            _add_principal(
                session=session,
                user_name=f"paged.user{index:02d}",
                source_type="pagination-test",
            )
        session.commit()

    response = flask_test_client.get(
        "/principals/download?source_type=pagination-test&page_size=5&page_number=0"
    )

    assert len(_csv_rows(response)) == 25


def test_download_resources_csv_applies_filters(
    flask_test_client: FlaskClient, database_empty: Database
):
    with database_empty.Session.begin() as session:
        _add_resource(
            session=session,
            fq_name="filtered.sales.orders",
            platform="filter-trino",
            object_type="table",
            attributes={"FilterAccessLevel": "Commercial"},
        )
        _add_resource(
            session=session,
            fq_name="filtered.sales.orders.total",
            platform="filter-trino",
            object_type="column",
            attributes={"FilterAccessLevel": "Commercial"},
        )
        _add_resource(
            session=session,
            fq_name="filtered.hr.employees",
            platform="filter-trino",
            object_type="table",
            attributes={"FilterAccessLevel": "Privacy"},
        )
        session.commit()

    response = flask_test_client.get(
        "/resources/download"
        "?platform=filter-trino"
        "&object_type=table"
        "&attributes=FilterAccessLevel:Commercial"
    )

    assert {row["fq_name"] for row in _csv_rows(response)} == {"filtered.sales.orders"}


def test_download_matches_the_table_for_the_same_filters(
    flask_test_client: FlaskClient, database_empty: Database
):
    """
    The point of the shared query: whatever the table shows, the CSV contains.
    """
    with database_empty.Session.begin() as session:
        _add_principal(
            session=session,
            user_name="match.finance.ldap",
            source_type="match-ldap",
            attributes={"match_department": "finance"},
        )
        _add_principal(
            session=session,
            user_name="match.finance.scim",
            source_type="match-scim",
            attributes={"match_department": "finance"},
        )
        _add_principal(
            session=session,
            user_name="match.sales.ldap",
            source_type="match-ldap",
            attributes={"match_department": "sales"},
        )
        session.commit()

    query_string: str = "attributes=match_department:finance&source_type=match-ldap"

    table_response = flask_test_client.get(f"/principals/table?{query_string}")
    download_response = flask_test_client.get(f"/principals/download?{query_string}")

    assert table_response.status_code == 200
    table_html: str = table_response.get_data(as_text=True)
    assert "match.finance.ldap" in table_html
    assert "match.finance.scim" not in table_html
    assert "match.sales.ldap" not in table_html

    assert {row["user_name"] for row in _csv_rows(download_response)} == {
        "match.finance.ldap"
    }
