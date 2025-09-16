import os
import requests
import json
from moat.src._scripts.create_db import create_db
from moat.src.cli.src.cli import ingest
from click.testing import CliRunner
from database import Database
from database.src.database_seeder import DatabaseSeeder

"""
This script can be run after the DB and flask app are up to seed the DB via scim and ingestor
"""

create_db()


def populate_users():
    with open("moat/seed_data/principals.json") as f:
        for principal in json.load(f):
            scim_user: dict = {
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:User",
                    "urn:ietf:params:scim:custom",
                ],
                "userName": principal.get("username"),
                "name": {
                    "givenName": principal.get("first_name"),
                    "familyName": principal.get("last_name"),
                },
                "emails": [{"value": principal.get("email"), "primary": True}],
                "entitlements": [{"value": e} for e in principal.get("entitlements")],
                "urn:ietf:params:scim:custom": principal.get("attributes"),
                "active": True,
            }

            response = requests.post(
                "http://localhost:8000/api/scim/v2/Users",
                headers={"Authorization": "Bearer scim-token"},
                json=scim_user,
            )
            print(response.json())
            response.raise_for_status()


populate_users()

db: Database = Database()
db.connect(echo_statements=True)
database_seeder: DatabaseSeeder = DatabaseSeeder(db=db)
database_seeder.seed()

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
