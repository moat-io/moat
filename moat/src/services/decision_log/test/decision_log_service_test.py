# TODO
"""
# Write
"InsertIntoTable",
"UpdateTableColumns",
"DeleteFromTable",
"""


from ..src.decision_log_service import DecisionLogService


def test_parse_decision_log__execute_query():
    assert DecisionLogService.parse_decision_log(
        {
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
    ) == {
        "result": True,
        "decision_id": "38a0c45b-b33e-4eb2-9d6f-490b41f08383",
        "labels": {
            "environment": "prod",
            "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_external_resolve_ns": 583,
            "timer_rego_input_parse_ns": 7217997,
            "timer_rego_query_eval_ns": 4421873,
            "timer_server_handler_ns": 17086993,
        },
        "operation": "ExecuteQuery",
        "path": "moat/trino/allow",
        "timestamp": "2025-10-02T08:37:19.589844241Z",
        "username": "alice",
    }


def test_parse_decision_log__access_catalog():
    assert DecisionLogService.parse_decision_log(
        {
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
    ) == {
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
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_external_resolve_ns": 291,
            "timer_rego_input_parse_ns": 18541,
            "timer_rego_query_eval_ns": 572249,
            "timer_server_handler_ns": 609624,
        },
        "operation": "AccessCatalog",
        "path": "moat/trino/allow",
        "timestamp": "2025-10-02T08:37:19.611571274Z",
        "username": "alice",
    }


def test_parse_decision_log__select_from_columns():
    assert DecisionLogService.parse_decision_log(
        {
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
    ) == {
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
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 24625,
            "timer_rego_query_eval_ns": 26500,
            "timer_server_handler_ns": 66791,
        },
        "operation": "SelectFromColumns",
        "path": "moat/trino/allow",
        "schema": "jdbc",
        "table": "schemas",
        "timestamp": "2025-10-02T08:37:19.613046149Z",
        "username": "alice",
    }


def test_parse_decision_log__filter_schemas():
    assert DecisionLogService.parse_decision_log(
        {
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
                        {
                            "schema": {
                                "catalogName": "datalake",
                                "schemaName": "logistics",
                            }
                        },
                        {
                            "schema": {
                                "catalogName": "datalake",
                                "schemaName": "pg_catalog",
                            }
                        },
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
    ) == {
        "decision_id": "471a56c3-4206-4fe2-9a2f-c900371aa480",
        "labels": {
            "environment": "prod",
            "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_external_resolve_ns": 375,
            "timer_rego_input_parse_ns": 34584,
            "timer_rego_query_eval_ns": 407209,
            "timer_server_handler_ns": 459583,
        },
        "operation": "FilterSchemas",
        "path": "moat/trino/batch",
        "timestamp": "2025-10-02T08:37:19.845323394Z",
        "username": "alice",
    }


def test_parse_decision_log__get_column_mask():
    # column mask batch
    assert DecisionLogService.parse_decision_log(
        {
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
    ) == {
        "labels": {
            "environment": "prod",
            "id": "b4f9e6cd-5ab5-472d-af7a-1c2e228e32b6",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_external_resolve_ns": 250,
            "timer_rego_input_parse_ns": 50625,
            "timer_rego_query_eval_ns": 112209,
            "timer_server_handler_ns": 191375,
        },
        "decision_id": "3771d0ce-ce8c-4259-aa20-424bb912001c",
        "path": "moat/trino/batchcolumnmask",
        "operation": "GetColumnMask",
        "timestamp": "2025-10-02T08:37:19.60712436Z",
        "username": "alice",
    }


def test_parse_decision_log_accesscatalog():
    assert (
        DecisionLogService.parse_decision_log(
            {
                "labels": {
                    "environment": "prod",
                    "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                    "instance": "user",
                    "platform": "trino",
                    "version": "1.8.0",
                },
                "decision_id": "5f08e249-b6b8-472c-8015-562ed0456a3a",
                "bundles": {"trino": {}},
                "path": "moat/trino/allow",
                "input": {
                    "action": {
                        "operation": "AccessCatalog",
                        "resource": {"catalog": {"name": "system"}},
                    },
                    "context": {
                        "identity": {"groups": [], "user": "admin"},
                        "softwareStack": {"trinoVersion": "476"},
                    },
                },
                "result": True,
                "requested_by": "10.89.1.7:45312",
                "timestamp": "2025-10-11T21:52:02.568968185Z",
                "metrics": {
                    "counter_server_query_cache_hit": 1,
                    "timer_rego_input_parse_ns": 19333,
                    "timer_rego_query_eval_ns": 28208,
                    "timer_server_handler_ns": 68000,
                },
                "req_id": 148,
            }
        )
        is not None
    )


def test_parse_decision_log_selectfromcolumns():
    assert (
        DecisionLogService.parse_decision_log(
            {
                "labels": {
                    "environment": "prod",
                    "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                    "instance": "user",
                    "platform": "trino",
                    "version": "1.8.0",
                },
                "decision_id": "5a59e699-3c94-4e38-8a8d-896ea3785b80",
                "bundles": {"trino": {}},
                "path": "moat/trino/allow",
                "input": {
                    "action": {
                        "operation": "SelectFromColumns",
                        "resource": {
                            "table": {
                                "catalogName": "system",
                                "columns": [
                                    "create_params",
                                    "type_name",
                                    "nullable",
                                    "auto_increment",
                                    "num_prec_radix",
                                    "precision",
                                    "minimum_scale",
                                    "sql_data_type",
                                    "maximum_scale",
                                    "literal_prefix",
                                    "searchable",
                                    "literal_suffix",
                                    "case_sensitive",
                                    "fixed_prec_scale",
                                    "data_type",
                                    "local_type_name",
                                    "sql_datetime_sub",
                                    "unsigned_attribute",
                                ],
                                "schemaName": "jdbc",
                                "tableName": "types",
                            }
                        },
                    },
                    "context": {
                        "identity": {"groups": [], "user": "admin"},
                        "softwareStack": {"trinoVersion": "476"},
                    },
                },
                "result": True,
                "requested_by": "10.89.1.7:45312",
                "timestamp": "2025-10-11T21:52:02.570458766Z",
                "metrics": {
                    "counter_server_query_cache_hit": 1,
                    "timer_rego_input_parse_ns": 24542,
                    "timer_rego_query_eval_ns": 16750,
                    "timer_server_handler_ns": 53084,
                },
                "req_id": 149,
            }
        )
        is not None
    )


def test_parse_decision_log_filtercatalogs():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "4489ca6a-63b2-47ff-8166-ba4f7fcaf34e",
            "bundles": {"trino": {}},
            "path": "moat/trino/batch",
            "input": {
                "action": {
                    "filterResources": [
                        {"catalog": {"name": "system"}},
                        {"catalog": {"name": "memory"}},
                        {"catalog": {"name": "workspace"}},
                        {"catalog": {"name": "datalake"}},
                        {"catalog": {"name": "moat"}},
                    ],
                    "operation": "FilterCatalogs",
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": [0, 1, 2, 3, 4],
            "requested_by": "10.89.1.7:45312",
            "timestamp": "2025-10-11T21:52:02.875765232Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 32042,
                "timer_rego_query_eval_ns": 219875,
                "timer_server_handler_ns": 1361164,
            },
            "req_id": 154,
        }
    ) == {
        "decision_id": "4489ca6a-63b2-47ff-8166-ba4f7fcaf34e",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 32042,
            "timer_rego_query_eval_ns": 219875,
            "timer_server_handler_ns": 1361164,
        },
        "operation": "FilterCatalogs",
        "path": "moat/trino/batch",
        "timestamp": "2025-10-11T21:52:02.875765232Z",
        "username": "admin",
    }


def test_parse_decision_log_filtertables():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "6104e364-1cf8-4cbd-b145-11ad21145ef8",
            "bundles": {"trino": {}},
            "path": "moat/trino/batch",
            "input": {
                "action": {
                    "filterResources": [
                        {
                            "table": {
                                "catalogName": "datalake",
                                "schemaName": "hr",
                                "tableName": "employees",
                            }
                        }
                    ],
                    "operation": "FilterTables",
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": [0],
            "requested_by": "10.89.1.7:45312",
            "timestamp": "2025-10-11T21:52:04.075778835Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 12416,
                "timer_rego_query_eval_ns": 32458,
                "timer_server_handler_ns": 55458,
            },
            "req_id": 168,
        }
    ) == {
        "decision_id": "6104e364-1cf8-4cbd-b145-11ad21145ef8",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 12416,
            "timer_rego_query_eval_ns": 32458,
            "timer_server_handler_ns": 55458,
        },
        "operation": "FilterTables",
        "path": "moat/trino/batch",
        "timestamp": "2025-10-11T21:52:04.075778835Z",
        "username": "admin",
    }


def test_parse_decision_log_filtercolumns():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "c0878fbd-f470-436c-9aa0-32e9be3c2f12",
            "bundles": {"trino": {}},
            "path": "moat/trino/batch",
            "input": {
                "action": {
                    "filterResources": [
                        {
                            "table": {
                                "catalogName": "datalake",
                                "columns": [
                                    "id",
                                    "firstname",
                                    "lastname",
                                    "email",
                                    "phonenumber",
                                ],
                                "schemaName": "hr",
                                "tableName": "employees",
                            }
                        }
                    ],
                    "operation": "FilterColumns",
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": [0, 1, 2, 3, 4],
            "requested_by": "10.89.1.7:45312",
            "timestamp": "2025-10-11T21:52:04.356879756Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 25792,
                "timer_rego_query_eval_ns": 337083,
                "timer_server_handler_ns": 380457,
            },
            "req_id": 178,
        }
    ) == {
        "decision_id": "c0878fbd-f470-436c-9aa0-32e9be3c2f12",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 25792,
            "timer_rego_query_eval_ns": 337083,
            "timer_server_handler_ns": 380457,
        },
        "operation": "FilterColumns",
        "path": "moat/trino/batch",
        "timestamp": "2025-10-11T21:52:04.356879756Z",
        "username": "admin",
    }


def test_parse_decision_log_showcreatetable():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "45c8be29-d2b5-460e-b813-c99c720830f0",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "ShowCreateTable",
                    "resource": {
                        "table": {
                            "catalogName": "datalake",
                            "schemaName": "hr",
                            "tableName": "employees",
                        }
                    },
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": True,
            "requested_by": "10.89.1.7:40570",
            "timestamp": "2025-10-11T22:45:53.732273931Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 18833,
                "timer_rego_query_eval_ns": 21125,
                "timer_server_handler_ns": 56875,
            },
            "req_id": 261,
        }
    ) == {
        "columns": "",
        "database": "datalake",
        "decision_id": "45c8be29-d2b5-460e-b813-c99c720830f0",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 18833,
            "timer_rego_query_eval_ns": 21125,
            "timer_server_handler_ns": 56875,
        },
        "operation": "ShowCreateTable",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "employees",
        "timestamp": "2025-10-11T22:45:53.732273931Z",
        "username": "admin",
    }


def test_parse_decision_log_impersonate_user():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "45c8be29-d2b5-460e-b813-c99c720830f0",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "ImpersonateUser",
                    "resource": {
                        "user": {
                            "user": "frank",
                        }
                    },
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": True,
            "requested_by": "10.89.1.7:40570",
            "timestamp": "2025-10-11T22:45:53.732273931Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 18833,
                "timer_rego_query_eval_ns": 21125,
                "timer_server_handler_ns": 56875,
            },
            "req_id": 261,
        }
    ) == {
        "decision_id": "45c8be29-d2b5-460e-b813-c99c720830f0",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 18833,
            "timer_rego_query_eval_ns": 21125,
            "timer_server_handler_ns": 56875,
        },
        "operation": "ImpersonateUser",
        "path": "moat/trino/allow",
        "result": True,
        "timestamp": "2025-10-11T22:45:53.732273931Z",
        "username": "admin",
        "value": "frank",
    }


def test_parse_decision_log_session_props():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "ef8e4ab9-a060-470f-a4d6-d8e5fa9ecb48",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "SetSystemSessionProperty",
                    "resource": {
                        "systemSessionProperty": {"name": "query_max_execution_time"}
                    },
                },
                "context": {
                    "identity": {"user": "admin"},
                    "softwareStack": {"trinoVersion": "468-e.2"},
                },
            },
            "result": True,
            "requested_by": "10.10.140.210:41611",
            "timestamp": "2025-10-06T03:08:26.98681908Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 91363,
                "timer_rego_query_eval_ns": 38720,
                "timer_server_handler_ns": 153959,
            },
            "req_id": 151,
        }
    ) == {
        "decision_id": "ef8e4ab9-a060-470f-a4d6-d8e5fa9ecb48",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 91363,
            "timer_rego_query_eval_ns": 38720,
            "timer_server_handler_ns": 153959,
        },
        "operation": "SetSystemSessionProperty",
        "path": "moat/trino/allow",
        "result": True,
        "timestamp": "2025-10-06T03:08:26.98681908Z",
        "username": "admin",
        "value": "query_max_execution_time",
    }
