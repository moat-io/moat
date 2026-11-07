package moat.trino

import rego.v1

test_filter_tables_batch if {
  actual := batch with input as {
    "action": {
      "operation": "FilterTables",
      "filterResources": [
        {
          "table": {
            "catalogName": "datalake",
            "schemaName": "hr",
            "tableName": "employees"
          }
        },
        {
          "table": {
            "catalogName": "datalake",
            "schemaName": "sales",
            "tableName": "orders"
          }
        }
      ]
    },
    "context": {
      "identity": {
        "user": "alice"
      },
      "softwareStack": {
        "Version": "0.1.0"
      }
    }
  }

  actual == {1}
}
