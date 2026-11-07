package moat.trino

import rego.v1

test_input_table if {
  actual := input_table with input as {
    "action": {
      "resource": {
        "table": {
          "catalogName": "datalake",
          "schemaName": "sales",
          "tableName": "customers"
        }
      }
    }
  }
  actual == "datalake.sales.customers"
}
