# Configuration Properties

This document lists all available configuration properties for Moat

## `bundle_generator.static_rego_file_path`
* Type: `string`
* Default: `opa/trino`
* Example: `opa/trino`

The path to static Rego files used for bundle generation.

## `bundle_generator.temp_directory`
* Type: `string`
* Default: `<none>`
* Example: `/tmp/moat-bundles`

The temporary directory for bundle generation.

## `common.db_connection_string`
* Type: `string`
* Default: `<none>`
* Example: `postgresql://user:password@localhost:5432/moat`

The database connection string for common operations.

## `common.super_secret`
* Type: `string`
* Default: `<none>`
* Example: `my-secret-key`

A secret value used for common operations.

## `connector.json_file.principals.file_path`
* Type: `string`
* Default: `<none>`
* Example: `/data/principals.json`

The path to the JSON file containing principal data.

## `database.database`
* Type: `string`
* Default: `<none>`
* Example: `moat`

The database name.

## `database.host`
* Type: `string`
* Default: `<none>`
* Example: `localhost`

The database host.

## `database.password`
* Type: `string`
* Default: `<none>`
* Example: `password`

The database password.

## `database.port`
* Type: `integer`
* Default: `<none>`
* Example: `5432`

The database port.

## `database.protocol`
* Type: `string`
* Default: `<none>`
* Example: `postgresql`

The database protocol.

## `database.seed_data_path`
* Type: `string`
* Default: `<none>`
* Example: `/data/seed`

The path to seed data for the database.

## `database.user`
* Type: `string`
* Default: `<none>`
* Example: `moat`

The database user.

## `dbapi_connector.attribute_key_key`
* Type: `string`
* Default: `attribute_key`
* Example: `attribute_key`

The key for attribute key in DBAPI connector.

## `dbapi_connector.attribute_value_key`
* Type: `string`
* Default: `attribute_value`
* Example: `attribute_value`

The key for attribute value in DBAPI connector.

## `dbapi_connector.client_type`
* Type: `string`
* Default: `<none>`
* Example: `trino`

The type of DBAPI client.

## `dbapi_connector.data_object_table_column_query`
* Type: `string`
* Default: `<none>`
* Example: `SELECT table_name, column_name FROM information_schema.columns`

The query to get data object table columns.

## `dbapi_connector.fq_name_key`
* Type: `string`
* Default: `fq_name`
* Example: `fq_name`

The key for fully qualified name in DBAPI connector.

## `dbapi_connector.object_type_key`
* Type: `string`
* Default: `object_type`
* Example: `object_type`

The key for object type in DBAPI connector.

## `ldap_client.base_dn`
* Type: `string`
* Default: `<none>`
* Example: `dc=example,dc=com`

The LDAP base DN.

## `ldap_client.host`
* Type: `url`
* Default: `<none>`
* Example: `ldap.domain.com`

The LDAP server host.

## `ldap_client.password`
* Type: `string`
* Default: `<none>`
* Example: `$LDAP_PASSWORD`

The LDAP password.

## `ldap_client.port`
* Type: `integer`
* Default: `<none>`
* Example: `3890`

The LDAP server port.

## `ldap_client.user_base_dn`
* Type: `string`
* Default: `<none>`
* Example: `ou=people,dc=example,dc=com`

The LDAP user base DN.

## `ldap_client.user_dn`
* Type: `string`
* Default: `<none>`
* Example: `uid=admin,ou=people,dc=example,dc=com`

The LDAP user DN.

## `ldap_connector.attr_email`
* Type: `string`
* Default: `<none>`
* Example: `mail`

The LDAP attribute for email.

## `ldap_connector.attr_first_name`
* Type: `string`
* Default: `<none>`
* Example: `givenName`

The LDAP attribute for first name.

## `ldap_connector.attr_groups`
* Type: `string`
* Default: `<none>`
* Example: `memberOf`

The LDAP attribute for groups.

## `ldap_connector.attr_last_name`
* Type: `string`
* Default: `<none>`
* Example: `sn`

The LDAP attribute for last name.

## `ldap_connector.attr_user_id`
* Type: `string`
* Default: `<none>`
* Example: `uid`

The LDAP attribute for user ID.

## `ldap_connector.attr_user_name`
* Type: `string`
* Default: `<none>`
* Example: `cn`

The LDAP attribute for user name.

## `ldap_connector.group_name_regex`
* Type: `string`
* Default: `(.*)`
* Example: `cn=(.*),ou=groups,dc=example,dc=com`

The regex to extract group names from LDAP group DNs.

## `ldap_connector.user_search_base`
* Type: `string`
* Default: `<none>`
* Example: `ou=people,dc=example,dc=com`

The LDAP user search base.

## `ldap_connector.user_search_filter`
* Type: `string`
* Default: `<none>`
* Example: `(objectClass=person)`

The LDAP user search filter.

## `logger.root_level`
* Type: `enum`
* Values: `DEBUG`, `INFO`, `WARN`, `ERROR`
* Default: `INFO`
* Example: `DEBUG`

Sets the log level of the root logger. This applies to all loggers within Moat as they all inherit from the root logger.

## `logger.<name>_level`
* Type: `enum`
* Values: `DEBUG`, `INFO`, `WARN`, `ERROR`
* Default: `INFO`
* Example: `DEBUG`

Sets the log level for a specific logger named `<name>`.

## `opa_authz_provider.policy_file_path`
* Type: `string`
* Default: `<none>`
* Example: `opa/moat/authz.rego`

The path to the OPA policy file.

## `opa_authz_provider.policy_name`
* Type: `string`
* Default: `moat/authz`
* Example: `moat/authz`

The name of the OPA policy.

## `opa_client.hostname`
* Type: `string`
* Default: `localhost`
* Example: `opa.moat.svc.cluster.local`

The hostname of the OPA instance that Moat should connect to.

## `opa_client.path`
* Type: `path`
* Default: `/v1/data/moat/authz/allow`
* Example: `/v1/data/moat/authz/allow`

The path to the OPA API endpoint.

## `opa_client.port`
* Type: `string`
* Default: `8181`
* Example: `8181`

The port of the OPA instance.

## `opa_client.scheme`
* Type: `enum`
* Values: `https`, `http`
* Default: `http`
* Example: `https`

The scheme to use when connecting to OPA. For production deployments, HTTPS should be used.

## `opa_client.timeout_seconds`
* Type: `string`
* Default: `1`
* Example: `10`

The timeout in seconds for OPA client requests.

## `trino_client.host`
* Type: `string`
* Default: `<none>`
* Example: `trino.example.com`

The Trino server host.

## `trino_client.port`
* Type: `integer`
* Default: `<none>`
* Example: `8080`

The Trino server port.

## `trino_client.username`
* Type: `string`
* Default: `<none>`
* Example: `trino`

The Trino username.

## `trino_client.password`
* Type: `string`
* Default: `<none>`
* Example: `password`

A password used when connecting to Trino with basic auth.

## `trino_client.jwt_token`
* Type: `string`
* Default: `<none>`
* Example: `rbg...dnc`

A JWT token used to authenticate with Trino. If this is supplied, then password is ignored

## `trino_client.ssl_verify`
* Type: `bool`
* Default: `true`
* Example: `true|false`

If set to false, disables SSL hostname verification
