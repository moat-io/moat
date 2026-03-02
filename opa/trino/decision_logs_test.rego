package system.log

import rego.v1

test_mask_filter_tables_1 if {
  actual := mask with input as {
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
  expected := {
    "/input/context/identity/groups",
    {"op": "upsert", "path": "/input/action/resourceCount", "value": 1},
    "/input/action/filterResources",
    "/result"
  }
  actual == expected
}

test_mask_filter_tables_2 if {
  actual := mask with input as {
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
                      },
                  },
                  {
                      "table": {
                          "catalogName": "datalake",
                          "schemaName": "hr",
                          "tableName": "employeenames",
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
  expected := {
    "/input/action/filterResources",
    "/input/context/identity/groups",
    "/result",
    {"op": "upsert", "path": "/input/action/resourceCount", "value": 2}
  }
  actual == expected
}