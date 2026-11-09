from urllib.parse import parse_qs, urlsplit

from flask.testing import FlaskClient


def test_policies_table_pushes_view_state(
    flask_test_client: FlaskClient, monkeypatch
) -> None:
    from views.controllers import PoliciesController

    monkeypatch.setattr(PoliciesController, "get_all_policies", lambda: [])

    response = flask_test_client.get(
        "/policies/table?search_term=mask&page_number=1&page_size=10",
        headers={"HX-Request": "true"},
    )

    assert response.status_code == 200
    pushed_url = response.headers["HX-Push-Url"]
    assert urlsplit(pushed_url).path == "/policies/"
    assert parse_qs(urlsplit(pushed_url).query) == {
        "search_term": ["mask"],
        "sort_key": ["name"],
        "sort_ascending": ["true"],
        "page_number": ["1"],
        "page_size": ["10"],
    }
