package moat.trino

import rego.v1
import data.trino

# ----------------- column masking rules ---------------------
# the mask value should be applied when a user doesnt have access to it
# when a user doesn't have access, they will still get a true on the column
# if that column doesnt have a defined mask

principal_has_access_to_column(attributes) if {
  principal_has_all_required_attributes(attributes)
}

# input table name syntax is different in the column mask request
column_mask_input_table(column) := sprintf("%s.%s.%s", [
	column.catalogName,
	column.schemaName,
	column.tableName
])

columnmask := {"expression": mask} if {
  # get the object and column
  data_object := data_objects[column_mask_input_table(input.action.resource.column)]
	column := data_object.columns[input.action.resource.column.columnName]

	# true if not all attrs on the column are on the principal
	# therefore we should not return a mask
  not principal_has_access_to_column(column.attributes)

  # either return the mask or a default which is null
  mask := object.get(column, "mask", "NULL")
}

# batch column masking - this can be improved as it finds the same object every time
# ordering or batching to find by one object at a time would be faster
batchcolumnmask contains { "index": i, "viewExpression": { "expression": "NULL" }} if {
  some i
  column_resource := input.action.filterResources[i]

  # get the object and column
  data_object := data_objects[column_mask_input_table(column_resource.column)]
	column := data_object.columns[column_resource.column.columnName]

  # true if not all attrs on the column are on the principal
	# therefore we should not return a mask
  not principal_has_access_to_column(column.attributes)
}
