# OpenMetadata Connector (`om`)

This connector ingests principals/resources (and their attributes) from OpenMetadata REST APIs.

Set the ingestion connector name to `om`:

```bash
python -m cli ingest --connector-name om --object-type principal --platform openmetadata
```

## Example config

```yaml
om_connector.base_url: "http://localhost:8585"
om_connector.auth_token: "$OPENMETADATA_JWT"
om_connector.auth_header_name: "Authorization"
om_connector.auth_header_prefix: "Bearer"
om_connector.timeout_s: "30"
om_connector.page_size: "100"

om_connector.principal_endpoint: "/api/v1/users"
om_connector.principal_query_params: "limit=100&fields=profile,teams"
om_connector.principal_content_pattern: "$.data[*]"
om_connector.principal_after_jsonpath: "$.paging.after"

om_connector.resource_endpoint: "/api/v1/tables"
om_connector.resource_endpoints: "/api/v1/databases,/api/v1/databaseSchemas,/api/v1/tables"
om_connector.resource_query_params: "limit=100&fields=tags"
om_connector.resource_content_pattern: "$.data[*]"
om_connector.resource_after_jsonpath: "$.paging.after"
om_connector.resource_object_type_default: "table"
om_connector.resource_type_mapping: "regular=table,external=table,view=table,materializedview=table,materialized_view=table,iceberg=table"

om_connector.principal_fq_name_jsonpath: "$.name"
om_connector.principal_first_name_jsonpath: "$.profile.firstName"
om_connector.principal_last_name_jsonpath: "$.profile.lastName"
om_connector.principal_user_name_jsonpath: "$.name"
om_connector.principal_email_jsonpath: "$.email"
om_connector.principal_fq_name_regex: ".*"
om_connector.principal_first_name_regex: ".*"
om_connector.principal_last_name_regex: ".*"
om_connector.principal_user_name_regex: ".*"
om_connector.principal_email_regex: ".*"

om_connector.principal_attribute_fq_name_jsonpath: "$.name"
om_connector.principal_attribute_attributes_multi_jsonpath: "$.teams[*]"
om_connector.principal_attribute_attributes_multi_regex: "(?P<key>[^:]+):(?P<value>.+)"
om_connector.principal_attribute_fq_name_regex: ".*"

om_connector.resource_fq_name_jsonpath: "$.fullyQualifiedName"
om_connector.resource_object_type_jsonpath: "$.tableType"
om_connector.resource_fq_name_regex: ".*"
om_connector.resource_object_type_regex: ".*"

om_connector.resource_attribute_fq_name_jsonpath: "$.fullyQualifiedName"
om_connector.resource_attribute_attributes_multi_jsonpath: "$.tags[*].tagFQN"
om_connector.resource_attribute_attributes_multi_regex: "(?P<key>[^.]+)\\.(?P<value>.+)"
om_connector.resource_attribute_fq_name_regex: ".*"
```

`resource_endpoints` lets one run ingest databases, schemas, and tables in one pass. `resource_type_mapping` normalizes OpenMetadata table type values (for example `View` / `MaterializedView`) to Moat `object_type` values.

`*_attribute_endpoint` keys are optional. If omitted, the connector derives attributes from the corresponding principal/resource payload.
