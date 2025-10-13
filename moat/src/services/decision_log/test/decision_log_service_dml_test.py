from ..src.decision_log_service import DecisionLogService


def test_parse_decision_log__insert():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "5fabcc10-bc07-4a8c-93cb-40e32bf029d5",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "InsertIntoTable",
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
            "requested_by": "10.89.1.7:58732",
            "timestamp": "2025-10-12T09:43:20.594502916Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 21333,
                "timer_rego_query_eval_ns": 24708,
                "timer_server_handler_ns": 65750,
            },
            "req_id": 509,
        }
    ) == {
        "columns": "",
        "database": "memory",
        "decision_id": "5fabcc10-bc07-4a8c-93cb-40e32bf029d5",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 21333,
            "timer_rego_query_eval_ns": 24708,
            "timer_server_handler_ns": 65750,
        },
        "operation": "InsertIntoTable",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "t_employees",
        "timestamp": "2025-10-12T09:43:20.594502916Z",
        "username": "admin",
    }


def test_parse_decision_log__update():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "e970bd1b-6646-4c97-9c6f-593a2b443d45",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "UpdateTableColumns",
                    "resource": {
                        "table": {
                            "catalogName": "memory",
                            "columns": ["id"],
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
            "requested_by": "10.89.1.7:55872",
            "timestamp": "2025-10-12T09:46:12.350732262Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 52334,
                "timer_rego_query_eval_ns": 51958,
                "timer_server_handler_ns": 145541,
            },
            "req_id": 519,
        }
    ) == {
        "columns": "id",
        "database": "memory",
        "decision_id": "e970bd1b-6646-4c97-9c6f-593a2b443d45",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 52334,
            "timer_rego_query_eval_ns": 51958,
            "timer_server_handler_ns": 145541,
        },
        "operation": "UpdateTableColumns",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "t_employees",
        "timestamp": "2025-10-12T09:46:12.350732262Z",
        "username": "admin",
    }


def test_parse_decision_log__delete():
    assert DecisionLogService.parse_decision_log(
        {
            "labels": {
                "environment": "prod",
                "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
                "instance": "user",
                "platform": "trino",
                "version": "1.8.0",
            },
            "decision_id": "c8dc2dfc-c0e0-420c-8dc1-5a58cc424171",
            "bundles": {"trino": {}},
            "path": "moat/trino/allow",
            "input": {
                "action": {
                    "operation": "DeleteFromTable",
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
            "requested_by": "10.89.1.7:50314",
            "timestamp": "2025-10-12T09:47:33.853976108Z",
            "metrics": {
                "counter_server_query_cache_hit": 1,
                "timer_rego_input_parse_ns": 20292,
                "timer_rego_query_eval_ns": 37916,
                "timer_server_handler_ns": 74833,
            },
            "req_id": 527,
        }
    ) == {
        "columns": "",
        "database": "memory",
        "decision_id": "c8dc2dfc-c0e0-420c-8dc1-5a58cc424171",
        "labels": {
            "environment": "prod",
            "id": "90532938-66f9-47a1-b1dd-886ea7be64b7",
            "instance": "user",
            "platform": "trino",
            "version": "1.8.0",
        },
        "metrics": {
            "counter_server_query_cache_hit": 1,
            "timer_rego_input_parse_ns": 20292,
            "timer_rego_query_eval_ns": 37916,
            "timer_server_handler_ns": 74833,
        },
        "operation": "DeleteFromTable",
        "path": "moat/trino/allow",
        "result": True,
        "schema": "hr",
        "table": "t_employees",
        "timestamp": "2025-10-12T09:47:33.853976108Z",
        "username": "admin",
    }
