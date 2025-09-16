package moat.trino

import rego.v1
import data.trino

data_objects := data.trino.data_objects
principals := data.trino.principals

input_principal_name := input.context.identity.user
action := input.action.operation

input_table := {
	"database": input.action.resource.table.catalogName,
	"schema": input.action.resource.table.schemaName,
	"table": input.action.resource.table.tableName
}

input_columns := input.action.resource.table.columns

data_object_attributes contains attribute if {
  some data_object in data_objects
	data_object.object == input_table
	some attribute in data_object.attributes
}

principal_attributes contains attribute if {
  some principal in principals
  principal.name == input_principal_name
  some attribute in principal.attributes
}

principal_exists(principal_name) if {
  principal_name
  some principal in principals
  principal.name == principal_name
}

data_object_is_tagged(data_object) if {
  data_object
  count(data_object.attributes) > 0
}

principal_has_all_required_attributes(required_principal_attributes) if {
  some required_attr_subject in required_principal_attributes
      startswith(required_attr_subject, "Subject::")
      subject := split(required_attr_subject, "::")[1]

  some required_attr_access_level in required_principal_attributes
    startswith(required_attr_access_level, "AccessLevel::")
    access_level := split(required_attr_access_level, "::")[1]

  # check principal has subject and access level
  concat("", [access_level, "::", subject]) in principal_attributes
}