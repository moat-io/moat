package moat.trino

import rego.v1
import data.input.input_filter_cols

test_input_empty if {
  not allow with input as {}
}

test_principal_exists if {
  principal_exists("bob")
}

test_not_principal_exists if {
  not principal_exists("boris")
}

test_execute_query_permission if {
  allow with input as {
      "context": {
          "identity": {
              "user": "anne"
          }
      },
      "action": {
          "operation": "ExecuteQuery"
      }
  }
}

# All valid users should have AccessCatalog on system
test_access_system_catalog_for_all_users if {
  allow with input as {
    "action": {
        "operation": "AccessCatalog",
        "resource": {
          "catalog": {
            "name": "system"
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

# all valid users should be able to show schemas on system
test_show_schema_in_system_for_all_users if {
  allow with input as {
    "action": {
      "operation": "ShowSchemas",
      "resource": {
        "catalog": {
          "name": "system"
        }
      }
    },
    "context": {
      "identity": {
        "user": "alice"
      }
    }
  }
}

# all valid users should have information_schema on all catalog
test_select_from_information_schema_for_all_users if {
  allow with input as {
    "action": {
      "operation": "FilterSchemas",
      "resource": {
        "schema": {
          "catalogName": "datalake",
          "schemaName": "information_schema"
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

# All valid users should have SelectFromColumns on system
test_select_from_system_catalog_for_all_users if {
  allow with input as {
      "context": {
          "identity": {
              "user": "alice"
          }
      },
      "action": {
          "operation": "SelectFromColumns",
          "resource": {
              "table": {
                  "catalogName": "system",
                  "schemaName": "jdbc",
                  "tableName": "types",
                  "columns": [
                      "create_params",
                      "type_name"
                  ]
              }
          }
      }
  }
}

# All valid users should have FilterCatalogs on system
test_filter_catalogs_system_catalog_for_all_users if {
  allow with input as {
    "action": {
      "operation": "FilterCatalogs",
      "resource": {
        "catalog": {
          "name": "system"
        }
      }
    },
    "context": {
      "identity": {
        "user": "alice"
      }
    }
  }
}

test_select_from_untagged_table if {
  not allow with input as {
      "context": {
          "identity": {
              "user": "alice"
          }
      },
      "action": {
          "operation": "SelectFromColumns",
          "resource": {
              "table": {
                  "catalogName": "memory",
                  "schemaName": "some_schema",
                  "tableName": "some_table",
                  "columns": [
                      "some_column"
                  ]
              }
          }
      }
  }
}

# TODO move this to anne
# should fail as the user doesnt have this tag
test_input_select_logistics_territories if {
  not allow with input as {
      "context": {
          "identity": {
              "user": "anne"
          }
      },
      "action": {
          "operation": "SelectFromColumns",
          "resource": {
              "table": {
                  "catalogName": "datalake",
                  "schemaName": "logistics",
                  "tableName": "territories",
                  "columns": [
                      "column1",
                      "column2",
                      "column3"
                  ]
              }
          }
      }
  }
}

# bob
# filter catalogs should fail for "memory"
test_bob_allow_catalogs_memory if {
  not allow with input as {
    "action": {
      "operation": "FilterCatalogs",
      "resource": {
        "catalog": {
          "name": "memory"
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

# filter catalogs should pass for "datalake"
test_bob_allow_catalogs_datalake if {
  allow with input as {
    "action": {
      "operation": "FilterCatalogs",
      "resource": {
        "catalog": {
          "name": "datalake"
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

# access catalogs should have the same behaviour as filter catalogs
test_bob_allow_catalogs if {
  not allow with input as {
    "action": {
      "operation": "AccessCatalog",
      "resource": {
        "catalog": {
          "name": "memory"
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

# filter catalogs should pass for "datalake"
test_bob_allow_catalogs if {
  allow with input as {
    "action": {
      "operation": "AccessCatalog",
      "resource": {
        "catalog": {
          "name": "datalake"
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

# filter schemas should allow hr for bob
test_bob_filter_schemas_in_datalake if {
  allow with input as {
    "action": {
      "operation": "FilterSchemas",
      "resource": {
        "schema": {
          "catalogName": "datalake",
          "schemaName": "hr"
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

test_alice_filter_schemas_in_datalake if {
  not allow with input as {
    "action": {
      "operation": "FilterSchemas",
      "resource": {
        "schema": {
          "catalogName": "datalake",
          "schemaName": "hr"
        }
      }
    },
    "context": {
      "identity": {
        "user": "alice"
      }
    }
  }
}

test_anne_filter_schemas_in_datalake if {
  allow with input as {
    "action": {
      "operation": "FilterSchemas",
      "resource": {
        "schema": {
          "catalogName": "datalake",
          "schemaName": "sales"
        }
      }
    },
    "context": {
      "identity": {
        "user": "anne"
      }
    }
  }
}


# filter tables
test_filter_tables_alice_hr_employees if {
  not allow with input as {
    "action": {
      "operation": "FilterTables",
      "resource": {
        "table": {
          "catalogName": "datalake",
          "schemaName": "hr",
          "tableName": "employees"
        }
      }
    },
    "context": {
      "identity": {
        "user": "alice"
      }
    }
  }
}

test_filter_tables_alice_logistics_shippers if {
  allow with input as {
    "action": {
      "operation": "FilterTables",
      "resource": {
        "table": {
          "catalogName": "datalake",
          "schemaName": "logistics",
          "tableName": "shippers"
        }
      }
    },
    "context": {
      "identity": {
        "user": "alice"
      }
    }
  }
}

test_filter_tables_alice_hr_employees if {
  not allow with input as {
    "action": {
      "operation": "FilterTables",
      "resource": {
        "table": {
          "catalogName": "datalake",
          "schemaName": "hr",
          "tableName": "employees"
        }
      }
    },
    "context": {
      "identity": {
        "user": "alice"
      }
    }
  }
}

# filter columns
test_filter_columns_alice_logistics_regions if {
  allow with input as {
      "action": {
        "operation": "FilterColumns",
        "resource": {
          "table": {
            "catalogName": "datalake",
            "columns": [
              "column1"
            ],
            "schemaName": "logistics",
            "tableName": "regions"
          }
        }
      },
      "context": {
        "identity": {
          "user": "alice"
        }
      }
    }
}

test_filter_columns_bob_hr_employees_column_a if {
  allow with input as {
      "action": {
        "operation": "FilterColumns",
        "resource": {
          "table": {
            "catalogName": "datalake",
            "columns": [
              "a"
            ],
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
}

# flip this to a not if we bring back column filtering
test_filter_columns_bob_hr_employees_column_phonenumber if {
  allow with input as {
      "action": {
        "operation": "FilterColumns",
        "resource": {
          "table": {
            "catalogName": "datalake",
            "columns": [
              "phonenumber"
            ],
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
}

# bob should be able to select from logistics.shippers, but not logistics.regions
test_bob_selects_logistics_shippers if {
  allow with input as {
      "context": {
          "identity": {
              "user": "bob"
          }
      },
      "action": {
          "operation": "SelectFromColumns",
          "resource": {
              "table": {
                  "catalogName": "datalake",
                  "schemaName": "logistics",
                  "tableName": "shippers",
                  "columns": [
                      "a",
                      "b",
                      "c"
                  ]
              }
          }
      }
  }
}

test_bob_selects_logistics_regions if {
  not allow with input as {
      "context": {
          "identity": {
              "user": "bob"
          }
      },
      "action": {
          "operation": "SelectFromColumns",
          "resource": {
              "table": {
                  "catalogName": "datalake",
                  "schemaName": "logistics",
                  "tableName": "regions",
                  "columns": [
                      "a",
                      "b",
                      "c"
                  ]
              }
          }
      }
  }
}

# bob should be able to select column a and column c from hr.employees
test_bob_selects_hr_employees_column_a_and_column_c if {
  allow with input as {
      "context": {
          "identity": {
              "user": "bob"
          }
      },
      "action": {
          "operation": "SelectFromColumns",
          "resource": {
              "table": {
                  "catalogName": "datalake",
                  "schemaName": "hr",
                  "tableName": "employees",
                  "columns": [
                      "a",
                      "c"
                  ]
              }
          }
      }
  }
}

# bob should be able to select column phonenumber from hr.employees even though it is masked
test_bob_selects_hr_employees_all_columns if {
  allow with input as {
      "context": {
          "identity": {
              "user": "bob"
          }
      },
      "action": {
          "operation": "SelectFromColumns",
          "resource": {
              "table": {
                  "catalogName": "datalake",
                  "schemaName": "hr",
                  "tableName": "employees",
                  "columns": [
                      "a",
                      "phonenumber",
                      "c"
                  ]
              }
          }
      }
  }
}

# frank should be able to select column b from hr.employees
test_frank_selects_hr_employees_all_columns if {
  allow with input as {
    "context": {
        "identity": {
            "user": "frank"
        }
    },
    "action": {
        "operation": "SelectFromColumns",
        "resource": {
            "table": {
                "catalogName": "datalake",
                "schemaName": "hr",
                "tableName": "employees",
                "columns": [
                    "a",
                    "b",
                    "c"
                ]
            }
        }
    }
  }
}

# alice should not be able to select column1 and column3 from hr.employees
test_alice_selects_hr_employees_column_a_and_column_c if {
  not allow with input as {
      "context": {
          "identity": {
              "user": "alice"
          }
      },
      "action": {
          "operation": "SelectFromColumns",
          "resource": {
              "table": {
                  "catalogName": "datalake",
                  "schemaName": "hr",
                  "tableName": "employees",
                  "columns": [
                      "a",
                      "c"
                  ]
              }
          }
      }
  }
}
