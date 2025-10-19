package system.log
import rego.v1

# remove groups
mask contains "/input/context/identity/groups"

# add in a resourceCount field for FilterTables, Schemas, Catalogs so we have some info without the entire list
mask contains {"op": "upsert", "path": "/input/action/resourceCount", "value": value} if {
  input.input.action.operation in ["FilterCatalogs", "FilterSchemas", "FilterTables", "FilterColumns"]
	value := count(input.input.action.filterResources)
}

# remove the list of tables in FilterTables, FilterColumns
mask contains "/input/action/filterResources" if {
  input.input.action.operation in ["FilterTables", "FilterColumns"]
}

mask contains "/result" if {
  input.input.action.operation in ["FilterTables", "FilterColumns"]
}

# drops decision logs that dont have any value
drop if {
	input.input.action.operation == "ExecuteQuery"
}