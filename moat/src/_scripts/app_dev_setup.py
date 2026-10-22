import os
import requests
import json
from models import ObjectTypeEnum
from _scripts.create_db import create_db
from cli.src.cli import ingest
from click.testing import CliRunner
from database import Database
from database.src.database_seeder import DatabaseSeeder

"""
This script can be run after the DB and flask app are up to seed the DB via scim and ingestor
"""

create_db()

SCIM_BASE_URL: str = "http://localhost:8000/api/scim/v2"
SCIM_HEADERS: dict = {"Authorization": "Bearer scim-token"}
CUSTOM_SCHEMA: str = "urn:ietf:params:scim:custom"


def populate_users():
    with open("moat/seed_data/principals.json") as f:
        for principal in json.load(f):
            scim_user: dict = {
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:User",
                    CUSTOM_SCHEMA,
                ],
                "userName": principal.get("username"),
                "name": {
                    "givenName": principal.get("first_name"),
                    "familyName": principal.get("last_name"),
                },
                "emails": [{"value": principal.get("email"), "primary": True}],
                "entitlements": [{"value": e} for e in principal.get("entitlements")],
                CUSTOM_SCHEMA: principal.get("attributes"),
                "active": True,
            }

            response = requests.post(
                f"{SCIM_BASE_URL}/Users",
                headers=SCIM_HEADERS,
                json=scim_user,
            )
            print(response.json())
            response.raise_for_status()


def get_user(user_name: str) -> dict:
    """
    The SCIM ID is generated on create, so users are looked up by their user name
    """
    response = requests.get(f"{SCIM_BASE_URL}/Users", headers=SCIM_HEADERS)
    response.raise_for_status()

    for scim_user in response.json().get("Resources", []):
        if scim_user.get("userName") == user_name:
            return scim_user

    raise ValueError(f"No SCIM user found with userName: {user_name}")


def patch_user(
    user_name: str,
    attributes: dict = None,
    remove_attribute_keys: list[str] = None,
    **fields,
) -> dict:
    """
    Patches a single SCIM user, i.e:

        patch_user("alice", attributes={"Department": "Sales"})
        patch_user("alice", remove_attribute_keys=["Privacy", "Redact"])
        patch_user("alice", active=False)

    The SCIM API exposes PUT rather than PATCH, so the stored payload is read back,
    the changes are applied to it and the whole user is replaced. Attributes that are
    absent from the replacement payload are deleted from the principal, which makes
    'remove_attribute_keys' the way to exercise principal attribute deletion.
    """
    scim_user: dict = get_user(user_name=user_name)

    custom_attributes: dict = dict(scim_user.get(CUSTOM_SCHEMA) or {})
    for attribute_key in remove_attribute_keys or []:
        custom_attributes.pop(attribute_key, None)
    custom_attributes.update(attributes or {})

    scim_user[CUSTOM_SCHEMA] = custom_attributes
    scim_user.update(fields)

    response = requests.put(
        f"{SCIM_BASE_URL}/Users/{scim_user.get('id')}",
        headers=SCIM_HEADERS,
        json=scim_user,
    )
    print(response.json())
    response.raise_for_status()
    return response.json()


def delete_user(user_name: str) -> None:
    scim_user: dict = get_user(user_name=user_name)

    response = requests.delete(
        f"{SCIM_BASE_URL}/Users/{scim_user.get('id')}",
        headers=SCIM_HEADERS,
    )
    response.raise_for_status()


db: Database = Database()
db.connect(echo_statements=True)
database_seeder: DatabaseSeeder = DatabaseSeeder(db=db)
database_seeder.seed(object_types=[ObjectTypeEnum.RESOURCE])

populate_users()
patch_user(user_name='bob', attributes={
      "Employee": "True",
      "Redact": "PII",
      "Privacy": ["Sales"],
      "Restricted": ["Sales", "HR"],
      "Commercial": ["Sales"]
    })
patch_user(user_name='bob', remove_attribute_keys=["Commercial"])
# delete_user(user_name='bob')

# os.environ["CONFIG_FILE_PATH"] = "moat/config/config.resource_ingestion.yaml"
# runner = CliRunner()
# runner.invoke(
#     ingest,
#     ["--connector-name", "dbapi", "--object-type", "resource", "--platform", "trino"],
# )
#
# os.environ["CONFIG_FILE_PATH"] = "moat/config/config.resource_attribute_ingestion.yaml"
# runner = CliRunner()
# runner.invoke(
#     ingest,
#     [
#         "--connector-name",
#         "dbapi",
#         "--object-type",
#         "resource_attribute",
#         "--platform",
#         "trino",
#     ],
# )
