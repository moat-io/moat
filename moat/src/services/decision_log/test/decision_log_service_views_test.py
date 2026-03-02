from ..src.decision_log_service import DecisionLogService


def test_parse_decision_log__create_view():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "caf4bfbe-502c-4805-856f-8ef42d913c71",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "CreateView",
                    "resource": {
                        "table": {
                            "catalogName": "datalake",
                            "schemaName": "hr",
                            "tableName": "v_employees",
                        }
                    },
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": True,
            "requested_by": "10.89.1.7:47814",
            "timestamp": "2025-10-11T22:47:42.902753957Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 31333,
                "timer_rego_query_eval_ns": 30583,
                "timer_server_handler_ns": 84125,
            },
            "req_id": 264,
        }
    ) == {
        "columns": "",
        "database": "datalake",
        "decision_id": "caf4bfbe-502c-4805-856f-8ef42d913c71",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 31333,
            "timer_rego_query_eval_ns": 30583,
            "timer_server_handler_ns": 84125,
        },
        "operation": "CreateView",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "v_employees",
        "timestamp": "2025-10-11T22:47:42.902753957Z",
        "username": "admin",
    }


def test_parse_decision_log__show_create_view():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "609b7ad7-c5d2-47fb-9fea-d83a264b97f9",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "ShowCreateTable",
                    "resource": {
                        "table": {
                            "catalogName": "memory",
                            "schemaName": "hr",
                            "tableName": "v_employees",
                        }
                    },
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": True,
            "requested_by": "10.89.1.7:40354",
            "timestamp": "2025-10-11T22:54:30.560672934Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 17583,
                "timer_rego_query_eval_ns": 33459,
                "timer_server_handler_ns": 110291,
            },
            "req_id": 318,
        }
    ) == {
        "columns": "",
        "database": "memory",
        "decision_id": "609b7ad7-c5d2-47fb-9fea-d83a264b97f9",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 17583,
            "timer_rego_query_eval_ns": 33459,
            "timer_server_handler_ns": 110291,
        },
        "operation": "ShowCreateTable",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "v_employees",
        "timestamp": "2025-10-11T22:54:30.560672934Z",
        "username": "admin",
    }


def test_parse_decision_log__drop_view():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "af1e1889-a59f-46d9-bda6-50d1d5e891cf",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "DropView",
                    "resource": {
                        "table": {
                            "catalogName": "memory",
                            "schemaName": "hr",
                            "tableName": "v_employees",
                        }
                    },
                },
                "context": {
                    "identity": {"groups": [], "user": "admin"},
                    "softwareStack": {"trinoVersion": "476"},
                },
            },
            "result": True,
            "requested_by": "10.89.1.7:40354",
            "timestamp": "2025-10-11T22:53:27.822894922Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 24125,
                "timer_rego_query_eval_ns": 27875,
                "timer_server_handler_ns": 97458,
            },
            "req_id": 307,
        }
    ) == {
        "columns": "",
        "database": "memory",
        "decision_id": "af1e1889-a59f-46d9-bda6-50d1d5e891cf",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 24125,
            "timer_rego_query_eval_ns": 27875,
            "timer_server_handler_ns": 97458,
        },
        "operation": "DropView",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "v_employees",
        "timestamp": "2025-10-11T22:53:27.822894922Z",
        "username": "admin",
    }
