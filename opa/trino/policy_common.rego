package moat.trino

import rego.v1
import data.trino

# ExecuteQuery comes with no parameters
allow if {
  # ensure we have a valid user
  input.action.operation == "ExecuteQuery"
  principal_exists(input_principal_name)
}

# show schemas/tables allowed globally for simplicity
allow if {
  input.action.operation in ["ShowSchemas", "ShowTables"]
}

# All valid users should have AccessCatalog/FilterCatalogs on system
allow if {
  input.action.resource.catalog.name == "system"
  input.action.operation in ["AccessCatalog", "FilterCatalogs"]
}

# All valid users should have SelectFromColumns on information_schema in all catalogs
allow if {
  input.action.resource.table.schemaName == "information_schema"
  input.action.operation in ["SelectFromColumns", "FilterTables"]
}

# All valid users should have SelectFromColumns and FilterTables on all tables in system
allow if {
  input.action.resource.table.catalogName == "system"
  input.action.operation in ["SelectFromColumns", "FilterTables"]
}


# filter catalogs
# user should have permissions on >0 objects in the target catalog
allow if {
  input.action.operation in ["FilterCatalogs", "AccessCatalog"]
  some data_object in data_objects
	data_object.object.database == input.action.resource.catalog.name
	data_object_is_tagged(data_object)
	principal_has_all_required_attributes(data_object.attributes)
}

# filter schemas - all users see information_schema
allow if {
  input.action.operation == "FilterSchemas"
  input.action.resource.schema.schemaName == "information_schema"
}

# filter schemas - all users see all schemas in system catalog
allow if {
  input.action.operation == "FilterSchemas"
  input.action.resource.schema.catalogName == "system"
}

allow if {
  input.action.operation == "FilterSchemas"
  some data_object in data_objects
	data_object.object.database == input.action.resource.schema.catalogName
	data_object.object.schema == input.action.resource.schema.schemaName
	data_object_is_tagged(data_object)
	principal_has_all_required_attributes(data_object.attributes)
}

# filter tables
allow if {
  input.action.operation == "FilterTables"
  some data_object in data_objects
	data_object.object == input_table
	data_object_is_tagged(data_object)
	principal_has_all_required_attributes(data_object.attributes)
}

# filter columns - all allowed as masking is default when inaccessible
allow if {
  input.action.operation == "FilterColumns"
}

# running the select
# we know the user exists from ExecuteQuery
allow if {
  input.action.operation == "SelectFromColumns"

  # ensure the object is tagged - untagged is otherwise free-for-all
  some data_object in data_objects
	data_object.object == input_table
  data_object_is_tagged(data_object)

  # ensure all attrs on object exist on principal
	principal_has_all_required_attributes(data_object.attributes)
}