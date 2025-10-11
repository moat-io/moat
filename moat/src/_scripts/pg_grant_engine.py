"""
Method to sync grants to postgres from OPA / moat

Assumptions/Limitations:
* Select only
* No roles, only direct grants
* Object level grants only

1. Get the users from moat (opa?)
2. Sync the users to postgres
3. Get the allowed objects from moat (opa?)
4. Sync the allowed objects to postgres

# Dependencies
* Need a resource API
* resource/user delta APIs would be good
* sync status table to run deltas

"""

import requests
import psycopg

conn = psycopg.connect(
    host="localhost",
    port="5432",
    dbname="warehouse",
    user="postgres",
    password="password",
)
cur = conn.cursor()

# pull the users from the SCIM api
response = requests.get(
    "http://localhost:8000/api/scim/v2/Users?startIndex=1&count=10",
    headers={"Authorization": "Bearer scim-token"},
)

usernames: list[str] = [
    user.get("userName") for user in response.json().get("Resources")
]
print(f"Moat users: {usernames}")

# make sure the users exist in pg
sql = " SELECT rolname AS username FROM pg_roles"
result = cur.execute(sql).fetchall()
pg_users = [u[0] for u in result]
print(f"PG users: {pg_users}")

for moat_user in usernames:
    if moat_user not in pg_users:
        sql = f"CREATE USER {moat_user} WITH PASSWORD 'password'"
        cur.execute(sql)
        print(f"Created user {moat_user}")
    else:
        print(f"User {moat_user} already exists")

conn.commit()

# get the objects from platform postgres
object_names: list[str] = ["warehouse.bronze.customers"]

for username in usernames:
    for object_name in object_names:
        database: str = object_name.split(".")[0]
        schema: str = object_name.split(".")[1]
        table: str = object_name.split(".")[2]

        opa_request = {
            "input": {
                "context": {"identity": {"user": username}},
                "action": {
                    "operation": "SelectFromColumns",
                    "resource": {
                        "table": {
                            "catalogName": database,
                            "schemaName": schema,
                            "tableName": table,
                        }
                    },
                },
            }
        }

        response = requests.post(
            "http://localhost:8181/v1/data/moat/trino/allow", json=opa_request
        )
        result = response.json().get("result", False)
        if not result:
            print(f"User {username} does not have access to {object_name}")
            continue

        # if true, get allowed columns
        sql = f"SELECT column_name FROM information_schema.columns  WHERE table_schema = '{schema}' AND table_name = '{table}';"
        columns = [u[0] for u in cur.execute(sql)]

        masks: dict = {}
        for column in columns:
            opa_request = {
                "input": {
                    "action": {
                        "operation": "GetColumnMask",
                        "resource": {
                            "column": {
                                "catalogName": database,
                                "schemaName": schema,
                                "tableName": table,
                                "columnName": column,
                            }
                        },
                    },
                    "context": {"identity": {"user": username}},
                }
            }

            response = requests.post(
                "http://localhost:8181/v1/data/moat/trino/columnmask", json=opa_request
            )
            result = response.json().get("result", False)
            masks[column] = result

        print(f"Masks for {username}: {masks}")

        sql = f"GRANT SELECT ({','.join(k for k in masks.keys() if not masks[k])}) ON {schema}.{table} TO {username}"
        print(sql)
        cur.execute(sql)
