package moat.trino

import rego.v1

# ------------------ masked column -----------------------
# admin should have no masked columns
test_column_mask_admin_select_hr_employees if {
  not columnmask with input as {
    "action": {
      "operation": "GetColumnMask",
      "resource": {
        "column": {
          "catalogName": "datalake",
          "schemaName": "hr",
          "tableName": "employees",
          "columnName": "phonenumber",
          "columnType": "integer"
        }
      }
    },
    "context": {
      "identity": {
        "user": "admin"
      }
    }
  }
}

# frank should not have this column masked as he can access it
test_column_mask_frank_select_hr_employees if {
  not columnmask with input as {
    "action": {
      "operation": "GetColumnMask",
      "resource": {
        "column": {
          "catalogName": "datalake",
          "schemaName": "hr",
          "tableName": "employees",
          "columnName": "phonenumber",
          "columnType": "integer"
        }
      }
    },
    "context": {
      "identity": {
        "user": "frank"
      }
    }
  }
}

# bob should see a column mask here
test_column_mask_bob_select_hr_employees if {
  actual := columnmask with input as {
      "action": {
        "operation": "GetColumnMask",
        "resource": {
          "column": {
            "catalogName": "datalake",
            "columnName": "phonenumber",
            "columnType": "integer",
            "schemaName": "hr",
            "tableName": "employees"
          }
        }
      },
      "context": {
        "identity": {
          "user": "bob"
        }
      }
    }
  expected := {"expression":"NULL"}
  expected == actual
}

# ------------------ unmasked column -----------------------
# bob should see this column as he can access it
test_column_mask_bob_select_logistics_shippers if {
  not columnmask with input as {
    "action": {
      "operation": "GetColumnMask",
      "resource": {
        "column": {
          "catalogName": "datalake",
          "schemaName": "logistics",
          "tableName": "shippers",
          "columnName": "phone",
          "columnType": "integer"
        }
      }
    },
    "context": {
      "identity": {
        "user": "bob"
      }
    }
  }
}

# alice should see a NULL column mask as no mask is defined
test_column_mask_alice_select_logistics_shippers if {
  actual := columnmask with input as {
    "action": {
      "operation": "GetColumnMask",
      "resource": {
        "column": {
          "catalogName": "datalake",
          "schemaName": "logistics",
          "tableName": "shippers",
          "columnName": "phone",
          "columnType": "integer"
        }
      }
    },
    "context": {
      "identity": {
        "user": "alice"
      }
    }
  }
  expected := {"expression": "NULL"}
  expected == actual
}

# ------------------ batched mode  -----------------------
# bob should see "phone" unmasked and "phonenumber" masked
test_batchcolumnmask if {
  actual := batchcolumnmask with input as {
      "context": {
          "identity": {
              "user": "bob"
          }
      },
      "action": {
          "operation": "GetColumnMask",
          "filterResources": [
              {
                  "column": {
                    "catalogName": "datalake",
                    "schemaName": "logistics",
                    "tableName": "shippers",
                    "columnName": "phone",
                    "columnType": "integer"
                  }
              },
              {
                  "column": {
                    "catalogName": "datalake",
                    "columnName": "phonenumber",
                    "columnType": "integer",
                    "schemaName": "hr",
                    "tableName": "employees"
                  }
              }
          ]
      }
  }
  expected := {
    {
      "index": 1,
      "viewExpression": {
        "expression": "NULL"
      }
    }
  }
  expected == actual
}













