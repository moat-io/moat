import re

from database import Database
from flask.testing import FlaskClient
from models import (
    PrincipalAttributeDbo,
    PrincipalDbo,
    ResourceAttributeDbo,
    ResourceDbo,
)

# the database fixture is module scoped, so each test below uses its own
# attribute key to keep the dropdown it asserts on free of other tests' rows.


def _option_values(response) -> list[str]:
    """The selectable values of the rendered <option> list, blank prompt aside."""
    return [
        value
        for value in re.findall(
            r'<option value="([^"]*)"', response.get_data(as_text=True)
        )
        if value
    ]


def _add_principal(session, user_name: str, attributes: dict[str, str]) -> None:
    principal: PrincipalDbo = PrincipalDbo()
    principal.fq_name = user_name
    principal.first_name = "Filter"
    principal.last_name = "User"
    principal.user_name = user_name
    principal.email = f"{user_name}@mail.com"
    principal.source_type = "ldap"
    principal.active = True
    session.add(principal)

    for attribute_key, attribute_value in attributes.items():
        attribute: PrincipalAttributeDbo = PrincipalAttributeDbo()
        attribute.fq_name = user_name
        attribute.attribute_key = attribute_key
        attribute.attribute_value = attribute_value
        session.add(attribute)


def _add_resource(session, fq_name: str, attributes: dict[str, str]) -> None:
    resource: ResourceDbo = ResourceDbo()
    resource.fq_name = fq_name
    resource.platform = "trino"
    resource.object_type = "table"
    resource.active = True
    session.add(resource)

    for attribute_key, attribute_value in attributes.items():
        attribute: ResourceAttributeDbo = ResourceAttributeDbo()
        attribute.fq_name = fq_name
        attribute.attribute_key = attribute_key
        attribute.attribute_value = attribute_value
        session.add(attribute)


def test_principal_attribute_values_dropdown_lists_components_only(
    flask_test_client: FlaskClient, database_empty: Database
):
    """
    A multi valued attribute is offered one component at a time. The joined
    string is never a selectable value, and a component shared by several
    principals appears once.
    """
    with database_empty.Session.begin() as session:
        _add_principal(
            session=session,
            user_name="dropdown.one",
            attributes={"PrincipalDomain": "123,321,431"},
        )
        _add_principal(
            session=session,
            user_name="dropdown.two",
            attributes={"PrincipalDomain": "123"},
        )
        session.commit()

    response = flask_test_client.get(
        "/principals/attribute-values?attribute_key=PrincipalDomain"
    )

    assert response.status_code == 200
    assert _option_values(response) == ["123", "321", "431"]


def test_resource_attribute_values_dropdown_lists_components_only(
    flask_test_client: FlaskClient, database_empty: Database
):
    with database_empty.Session.begin() as session:
        _add_resource(
            session=session,
            fq_name="catalog.schema.dropdown_table",
            attributes={"ResourceDomain": "123,321,431"},
        )
        _add_resource(
            session=session,
            fq_name="catalog.schema.dropdown_other",
            attributes={"ResourceDomain": "431"},
        )
        session.commit()

    response = flask_test_client.get(
        "/resources/attribute-values?attribute_key=ResourceDomain"
    )

    assert response.status_code == 200
    assert _option_values(response) == ["123", "321", "431"]


def _table_user_names(response) -> set[str]:
    """The principals listed in the rendered table fragment."""
    html: str = response.get_data(as_text=True)
    return {name for name in ("match.one", "match.two", "match.three") if name in html}


def _seed_match_principals(database_empty: Database) -> None:
    """
    match.one    MatchTeam = 'HR,IT'   MatchTier = 'gold'
    match.two    MatchTeam = 'HR'      MatchTier = 'gold'
    match.three  MatchTeam = 'IT'      MatchTier = 'silver'
    """
    with database_empty.Session.begin() as session:
        _add_principal(
            session=session,
            user_name="match.one",
            attributes={"MatchTeam": "HR,IT", "MatchTier": "gold"},
        )
        _add_principal(
            session=session,
            user_name="match.two",
            attributes={"MatchTeam": "HR", "MatchTier": "gold"},
        )
        _add_principal(
            session=session,
            user_name="match.three",
            attributes={"MatchTeam": "IT", "MatchTier": "silver"},
        )
        session.commit()


def test_every_filter_row_must_match(
    flask_test_client: FlaskClient, database_empty: Database
):
    """
    Each row added narrows the result, rows on the same attribute included:
    only match.one carries both HR and IT.
    """
    _seed_match_principals(database_empty=database_empty)

    response = flask_test_client.get("/principals/table?attributes=MatchTeam:HR")
    assert _table_user_names(response) == {"match.one", "match.two"}

    response = flask_test_client.get(
        "/principals/table?attributes=MatchTeam:HR&attributes=MatchTeam:IT"
    )
    assert response.status_code == 200
    assert _table_user_names(response) == {"match.one"}


def test_rows_on_different_attributes_must_all_match(
    flask_test_client: FlaskClient, database_empty: Database
):
    """A row on a second attribute has to match as well, across separate rows."""
    response = flask_test_client.get(
        "/principals/table?attributes=MatchTeam:HR&attributes=MatchTier:gold"
    )
    assert _table_user_names(response) == {"match.one", "match.two"}

    response = flask_test_client.get(
        "/principals/table?attributes=MatchTeam:HR&attributes=MatchTier:silver"
    )
    assert response.status_code == 200
    assert _table_user_names(response) == set()


def test_principals_csv_download_carries_the_attribute_filters(
    flask_test_client: FlaskClient, database_empty: Database
):
    """
    The download replays the filters as applied, so the CSV cannot disagree with
    the table it was requested from.
    """
    rows = flask_test_client.get(
        "/principals/download"
        "?attributes=MatchTeam:HR&attributes=MatchTeam:IT&attributes=MatchTier:gold"
    ).get_data(as_text=True)

    assert "match.one" in rows
    assert "match.two" not in rows
    assert "match.three" not in rows


def test_table_response_redraws_the_applied_filter_rows(
    flask_test_client: FlaskClient, database_empty: Database
):
    """
    The rows are swapped out of band with the table, one per applied filter,
    each removable on its own. No operator is offered anywhere.
    """
    html = " ".join(
        flask_test_client.get(
            "/principals/table"
            "?attributes=MatchTeam:HR&attributes=MatchTeam:IT"
            "&attributes=MatchTier:gold"
        )
        .get_data(as_text=True)
        .split()
    )

    assert 'id="principals-attr-rows" hx-swap-oob="true"' in html
    for applied in ("MatchTeam:HR", "MatchTeam:IT", "MatchTier:gold"):
        assert f'data-value="{applied}"' in html, applied

    for operator_markup in ("attribute_match", "group_match", "(AND)", "(OR)"):
        assert operator_markup not in html, operator_markup


def test_filter_panel_renders_the_row_list_and_picker(
    flask_test_client: FlaskClient, database_empty: Database
):
    """Both views carry the row list and the attribute/value picker below it."""
    for url, prefix in (
        ("/principals/", "principals"),
        ("/resources/tables", "resources"),
    ):
        html = " ".join(
            flask_test_client.get(url, headers={"HX-Request": "true"})
            .get_data(as_text=True)
            .split()
        )

        assert f'id="{prefix}-attr-rows"' in html, url
        assert f'id="{prefix}-attr-key"' in html, url
        assert f'id="{prefix}-attr-value"' in html, url
        assert "+ Add filter" in html, url
        assert "Reset filters" in html, url
        # one value at a time, and no operator to choose
        assert "multiple" not in html, url
        assert "attribute_match" not in html, url
