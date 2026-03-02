from ..src.decision_log_service import DecisionLogService


def test_parse_decision_log__create_schema():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "ed4b185c-b957-41e3-a651-9f5f69226de3",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "CreateSchema",
                    "resource": {
                        "schema": {
                            "catalogName": "memory",
                            "properties": {},
                            "schemaName": "hr",
                        }
                    },
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": True,
            "requested_by": "10.89.1.7:55210",
            "timestamp": "2025-10-11T22:50:30.665763809Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 43917,
                "timer_rego_query_eval_ns": 46459,
                "timer_server_handler_ns": 124125,
            },
            "req_id": 279,
        }
    ) == {
        "columns": "",
        "database": "memory",
        "decision_id": "ed4b185c-b957-41e3-a651-9f5f69226de3",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 43917,
            "timer_rego_query_eval_ns": 46459,
            "timer_server_handler_ns": 124125,
        },
        "operation": "CreateSchema",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "",
        "timestamp": "2025-10-11T22:50:30.665763809Z",
        "username": "admin",
    }


def test_parse_decision_log__drop_schema():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "e4cdb9ed-1071-4479-8a0e-7a2413d5b8f5",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "DropSchema",
                    "resource": {
                        "schema": {"catalogName": "memory", "schemaName": "hr"}
                    },
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": True,
            "requested_by": "10.89.1.7:47170",
            "timestamp": "2025-10-11T22:56:34.035771434Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 39083,
                "timer_rego_query_eval_ns": 59541,
                "timer_server_handler_ns": 142125,
            },
            "req_id": 322,
        }
    ) == {
        "columns": "",
        "database": "memory",
        "decision_id": "e4cdb9ed-1071-4479-8a0e-7a2413d5b8f5",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 39083,
            "timer_rego_query_eval_ns": 59541,
            "timer_server_handler_ns": 142125,
        },
        "operation": "DropSchema",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "",
        "timestamp": "2025-10-11T22:56:34.035771434Z",
        "username": "admin",
    }


def test_parse_decision_log__create_table():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "1810ccf4-b3d9-48d7-ba87-5b369bbf4c9d",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "CreateTable",
                    "resource": {
                        "table": {
                            "catalogName": "memory",
                            "properties": {},
                            "schemaName": "hr",
                            "tableName": "t_employees",
                        }
                    },
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": True,
            "requested_by": "10.89.1.7:45380",
            "timestamp": "2025-10-11T22:58:12.702380189Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 24124,
                "timer_rego_query_eval_ns": 26917,
                "timer_server_handler_ns": 71583,
            },
            "req_id": 363,
        }
    ) == {
        "columns": "",
        "database": "memory",
        "decision_id": "1810ccf4-b3d9-48d7-ba87-5b369bbf4c9d",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 24124,
            "timer_rego_query_eval_ns": 26917,
            "timer_server_handler_ns": 71583,
        },
        "operation": "CreateTable",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "t_employees",
        "timestamp": "2025-10-11T22:58:12.702380189Z",
        "username": "admin",
    }


def test_parse_decision_log__drop_table():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "2c7fa2c3-ab8f-4885-8f86-6ac8aaec8858",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "DropTable",
                    "resource": {
                        "table": {
                            "catalogName": "memory",
                            "schemaName": "hr",
                            "tableName": "t_employees",
                        }
                    },
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": True,
            "requested_by": "10.89.1.7:45380",
            "timestamp": "2025-10-11T22:58:17.489799624Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 21125,
                "timer_rego_query_eval_ns": 24958,
                "timer_server_handler_ns": 65542,
            },
            "req_id": 369,
        }
    ) == {
        "columns": "",
        "database": "memory",
        "decision_id": "2c7fa2c3-ab8f-4885-8f86-6ac8aaec8858",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 21125,
            "timer_rego_query_eval_ns": 24958,
            "timer_server_handler_ns": 65542,
        },
        "operation": "DropTable",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "t_employees",
        "timestamp": "2025-10-11T22:58:17.489799624Z",
        "username": "admin",
    }
