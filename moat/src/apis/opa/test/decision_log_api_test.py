execute_query: dict = {
    "labels": {
        "environment": "prod",
        "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
        "instance": "user",
        "platform": "trino",
        "version": "1.8.0",
    },
    "decision_id": "38a0c45b-b33e-4eb2-9d6f-490b41f08383",
    "bundles": {"trino": {}},
    "path": "moat/trino/allow",
    "input": {
        "action": {"operation": "ExecuteQuery"},
        "context": {
            "identity": {"groups": [], "user": "alice"},
            "softwareStack": {"trinoVersion": "476"},
        },
    },
    "result": True,
    "requested_by": "10.89.1.6:38988",
    "timestamp": "2025-10-02T08:37:19.589844241Z",
    "metrics": {
        "counter_server_query_cache_hit": 1,
        "timer_rego_external_resolve_ns": 583,
        "timer_rego_input_parse_ns": 7217997,
        "timer_rego_query_eval_ns": 4421873,
        "timer_server_handler_ns": 17086993,
    },
    "req_id": 222,
}


access_catalog: dict = {
    "labels": {
        "environment": "prod",
        "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
        "instance": "user",
        "platform": "trino",
        "version": "1.8.0",
    },
    "decision_id": "0005300c-384e-4f0f-90bf-2df4ef23b5a2",
    "bundles": {"trino": {}},
    "path": "moat/trino/allow",
    "input": {
        "action": {
            "operation": "AccessCatalog",
            "resource": {"catalog": {"name": "system"}},
        },
        "context": {
            "identity": {"groups": [], "user": "alice"},
            "softwareStack": {"trinoVersion": "476"},
        },
    },
    "result": True,
    "requested_by": "10.89.1.6:38988",
    "timestamp": "2025-10-02T08:37:19.611571274Z",
    "metrics": {
        "counter_server_query_cache_hit": 1,
        "timer_rego_external_resolve_ns": 291,
        "timer_rego_input_parse_ns": 18541,
        "timer_rego_query_eval_ns": 572249,
        "timer_server_handler_ns": 609624,
    },
    "req_id": 224,
}

select_from_columns: dict = {
    "labels": {
        "environment": "prod",
        "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
        "instance": "user",
        "platform": "trino",
        "version": "1.8.0",
    },
    "decision_id": "293cb3cc-1168-4363-87c1-a963b0ad5432",
    "bundles": {"trino": {}},
    "path": "moat/trino/allow",
    "input": {
        "action": {
            "operation": "SelectFromColumns",
            "resource": {
                "table": {
                    "catalogName": "system",
                    "columns": ["table_schem", "table_catalog"],
                    "schemaName": "jdbc",
                    "tableName": "schemas",
                }
            },
        },
        "context": {
            "identity": {"groups": [], "user": "alice"},
            "softwareStack": {"trinoVersion": "476"},
        },
    },
    "result": True,
    "requested_by": "10.89.1.6:38988",
    "timestamp": "2025-10-02T08:37:19.613046149Z",
    "metrics": {
        "counter_server_query_cache_hit": 1,
        "timer_rego_input_parse_ns": 24625,
        "timer_rego_query_eval_ns": 26500,
        "timer_server_handler_ns": 66791,
    },
    "req_id": 225,
}


filter_catalogs: dict = {
    "labels": {
        "environment": "prod",
        "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
        "instance": "user",
        "platform": "trino",
        "version": "1.8.0",
    },
    "decision_id": "8672cd12-ceee-4c5d-9bf0-f989a01fa5da",
    "bundles": {"trino": {}},
    "path": "moat/trino/batch",
    "input": {
        "action": {
            "filterResources": [{"catalog": {"name": "datalake"}}],
            "operation": "FilterCatalogs",
        },
        "context": {
            "identity": {"groups": [], "user": "alice"},
            "softwareStack": {"trinoVersion": "476"},
        },
    },
    "result": [0],
    "requested_by": "10.89.1.6:38988",
    "timestamp": "2025-10-02T08:37:19.758685344Z",
    "metrics": {
        "counter_server_query_cache_hit": 1,
        "timer_rego_external_resolve_ns": 666,
        "timer_rego_input_parse_ns": 34083,
        "timer_rego_query_eval_ns": 2434666,
        "timer_server_handler_ns": 2495082,
    },
    "req_id": 226,
}


filter_schemas: dict = {
    "labels": {
        "environment": "prod",
        "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
        "instance": "user",
        "platform": "trino",
        "version": "1.8.0",
    },
    "decision_id": "471a56c3-4206-4fe2-9a2f-c900371aa480",
    "bundles": {"trino": {}},
    "path": "moat/trino/batch",
    "input": {
        "action": {
            "filterResources": [
                {
                    "schema": {
                        "catalogName": "datalake",
                        "schemaName": "information_schema",
                    }
                },
                {"schema": {"catalogName": "datalake", "schemaName": "hr"}},
                {"schema": {"catalogName": "datalake", "schemaName": "logistics"}},
                {"schema": {"catalogName": "datalake", "schemaName": "pg_catalog"}},
                {"schema": {"catalogName": "datalake", "schemaName": "public"}},
                {"schema": {"catalogName": "datalake", "schemaName": "sales"}},
            ],
            "operation": "FilterSchemas",
        },
        "context": {
            "identity": {"groups": [], "user": "alice"},
            "softwareStack": {"trinoVersion": "476"},
        },
    },
    "result": [0, 2, 5],
    "requested_by": "10.89.1.6:38988",
    "timestamp": "2025-10-02T08:37:19.845323394Z",
    "metrics": {
        "counter_server_query_cache_hit": 1,
        "timer_rego_external_resolve_ns": 375,
        "timer_rego_input_parse_ns": 34584,
        "timer_rego_query_eval_ns": 407209,
        "timer_server_handler_ns": 459583,
    },
    "req_id": 228,
}

batch_column_mask: dict = {
    "labels": {
        "environment": "prod",
        "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
        "instance": "user",
        "platform": "trino",
        "version": "1.8.0",
    },
    "decision_id": "3771d0ce-ce8c-4259-aa20-424bb912001c",
    "bundles": {"trino": {}},
    "path": "moat/trino/batchcolumnmask",
    "input": {
        "action": {
            "filterResources": [
                {
                    "column": {
                        "catalogName": "system",
                        "columnName": "table_schem",
                        "columnType": "varchar",
                        "schemaName": "jdbc",
                        "tableName": "schemas",
                    }
                },
                {
                    "column": {
                        "catalogName": "system",
                        "columnName": "table_catalog",
                        "columnType": "varchar",
                        "schemaName": "jdbc",
                        "tableName": "schemas",
                    }
                },
            ],
            "operation": "GetColumnMask",
        },
        "context": {
            "identity": {"groups": [], "user": "alice"},
            "softwareStack": {"trinoVersion": "476"},
        },
    },
    "result": [],
    "requested_by": "10.89.1.6:38988",
    "timestamp": "2025-10-02T08:37:19.60712436Z",
    "metrics": {
        "counter_server_query_cache_hit": 1,
        "timer_rego_external_resolve_ns": 250,
        "timer_rego_input_parse_ns": 50625,
        "timer_rego_query_eval_ns": 112209,
        "timer_server_handler_ns": 191375,
    },
    "req_id": 223,
}


from ..src.decision_log_api import _parse_decision_log


def test_parse_decision_log():
    assert _parse_decision_log(execute_query) == {
        "result": True,
        "decision_id": "38a0c45b-b33e-4eb2-9d6f-490b41f08383",
        "labels": {
            "environment": "prod",
            "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "operation": "ExecuteQuery",
        "path": "moat/trino/allow",
        "timestamp": "2025-10-02T08:37:19.589844241Z",
        "username": "alice",
    }

    assert _parse_decision_log(access_catalog) == {
        "result": True,
        "database": "system",
        "decision_id": "0005300c-384e-4f0f-90bf-2df4ef23b5a2",
        "labels": {
            "environment": "prod",
            "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "operation": "AccessCatalog",
        "path": "moat/trino/allow",
        "timestamp": "2025-10-02T08:37:19.611571274Z",
        "username": "alice",
    }

    assert _parse_decision_log(select_from_columns) == {
        "result": True,
        "columns": "table_schem, table_catalog",
        "database": "system",
        "decision_id": "293cb3cc-1168-4363-87c1-a963b0ad5432",
        "labels": {
            "environment": "prod",
            "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "operation": "SelectFromColumns",
        "path": "moat/trino/allow",
        "schema": "jdbc",
        "table": "schemas",
        "timestamp": "2025-10-02T08:37:19.613046149Z",
        "username": "alice",
    }

    assert _parse_decision_log(filter_catalogs) == {
        "database": "datalake",
        "decision_id": "8672cd12-ceee-4c5d-9bf0-f989a01fa5da",
        "result": "0",
        "labels": {
            "environment": "prod",
            "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "operation": "FilterCatalogs",
        "path": "moat/trino/batch",
        "timestamp": "2025-10-02T08:37:19.758685344Z",
        "username": "alice",
    }

    assert _parse_decision_log(filter_schemas) == {
        "decision_id": "471a56c3-4206-4fe2-9a2f-c900371aa480",
        "result": "0,2,5",
        "labels": {
            "environment": "prod",
            "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "operation": "FilterSchemas",
        "path": "moat/trino/batch",
        "schema": "datalake.information_schema,datalake.hr,datalake.logistics,datalake.pg_catalog,datalake.public,datalake.sales",
        "timestamp": "2025-10-02T08:37:19.845323394Z",
        "username": "alice",
    }

    # TODO
    """
    "GetColumnMask",
    "FilterTables",
    "FilterColumns",
    "CreateSchema",
    "DropSchema",
    "CreateTable",
    "DropTable",
    "ShowCreateTable",
    "InsertIntoTable",
    "UpdateTableColumns",
    "DeleteFromTable",
    """

    # column mask batch
    assert _parse_decision_log(batch_column_mask) == {
        "labels": {
            "environment": "prod",
            "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "decision_id": "3771d0ce-ce8c-4259-aa20-424bb912001c",
        "path": "moat/trino/batchcolumnmask",
        "operation": "GetColumnMask",
        "timestamp": "2025-10-02T08:37:19.60712436Z",
        "username": "alice",
    }
