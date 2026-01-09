package moat.trino

import rego.v1

# ------------------ batched mode  -----------------------
# Filter Columns has a corner case in its rego implementation

test_filtercolumns if {
  actual := batch with input as {
      "context": {
          "identity": {
              "user": "jo"
          }
      },
      "action": {
          "operation": "FilterColumns",
          "filterResources": [
              {
                  "table": {
                      "columns": [
                        "col1",
                        "col2",
                        "col3",
                        "col4"
                      ]
                  }
              }
          ]
      }
  }
  expected := {0, 1, 2, 3}
  expected == actual
}