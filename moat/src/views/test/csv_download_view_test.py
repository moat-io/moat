from flask.testing import FlaskClient

from database import Database
from models import PrincipalAttributeDbo, PrincipalDbo, ResourceDbo


def test_download_principals_csv(
    flask_test_client: FlaskClient, database_empty: Database
):
    with database_empty.Session.begin() as session:
        principal: PrincipalDbo = PrincipalDbo()
        principal.fq_name = "csv.user"
        principal.first_name = "Csv"
        principal.last_name = "User"
        principal.user_name = "csv.user"
        principal.email = "csv.user@mail.com"
        principal.source_type = "ldap"
        session.add(principal)

        attribute: PrincipalAttributeDbo = PrincipalAttributeDbo()
        attribute.fq_name = "csv.user"
        attribute.attribute_key = "department"
        attribute.attribute_value = "finance"
        session.add(attribute)
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
        resource: ResourceDbo = ResourceDbo()
        resource.fq_name = "datalake.sales.orders"
        resource.platform = "trino"
        resource.object_type = "table"
        session.add(resource)
        session.commit()

    response = flask_test_client.get("/resources/download")

    assert response.status_code == 200
    assert response.mimetype == "text/csv"
    assert (
        response.headers["Content-Disposition"] == "attachment; filename=resources.csv"
    )

    lines: list[str] = response.get_data(as_text=True).splitlines()
    assert lines[0] == "fq_name,platform,object_type,attributes"
    assert "datalake.sales.orders,trino,table," in lines
