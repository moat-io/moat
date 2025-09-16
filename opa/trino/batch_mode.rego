package moat.trino

import rego.v1
import data.trino


# batch mode - run with both the input and output resources if they exist
batch contains i if {
	some i
	raw_resource := input.action.filterResources[i]
	allow with input.action.resource as raw_resource
}

batch contains i if {
	some i
	input.action.operation == "FilterColumns"
	count(input.action.filterResources) == 1
	raw_resource := input.action.filterResources[0]
	count(raw_resource.table.columns) > 0
	new_resources := [
    object.union(raw_resource, {"table": {"column": column_name}}) | column_name := raw_resource.table.columns[_]
	]
	allow with input.action.resource as new_resources[i]
}
