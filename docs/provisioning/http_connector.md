# HTTP Connector

The HTTP Connector allows Moat to ingest user data from external HTTP/REST APIs. This document explains how to configure and use the connector to import users and their attributes into Moat.

## Overview

The HTTP Connector is designed to:

1. Connect to external HTTP APIs using various authentication methods
2. Retrieve user data from these APIs
3. Transform the data into Moat's internal data models
4. Import the transformed data into Moat

## CLI / Cronjob Usage
```bash
export CONFIG_FILE_PATH=moat/config/config.principal_ingestion.yaml
./entrypoint.sh ingest --connector-name=http --object-type=principal --platform=idp
./entrypoint.sh ingest --connector-name=http --object-type=principal_attribute --platform=idp
```

## Configuration

The HTTP Connector is configured through the `HttpConnectorConfig` class, which extends `AppConfigModelBase`. Configuration options are loaded from your application's configuration files with the prefix `http_connector`.

### Authentication Options

The connector supports multiple authentication methods:

- **No Authentication** (`auth_method: "none"`)
- **Basic Authentication** (`auth_method: "basic"`)
  - Requires `username` and `password`
- **API Key Authentication** (`auth_method: "api-key"`)
  - Requires `api_key`
- **OAuth2 Authentication** (`auth_method: "oauth2"`)
  - Requires:
    - `oauth2_endpoint`: Token endpoint URL
    - `oauth2_client_id`: Client ID
    - `oauth2_client_secret`: Client secret
    - `oauth2_grant_type`: Grant type (e.g., "client_credentials")
    - `oauth2_scope`: OAuth scopes (space-separated)

### Connection Options

- `url`: The API endpoint URL to fetch user data
- `ssl_verify`: Whether to verify SSL certificates (default: `true`)
- `certificate_path`: Path to SSL certificate (optional)

### Data Mapping Configuration

The connector uses JSONPath expressions and regex patterns to map data from the API response to Moat's internal data models. These mappings are configured with the following pattern:

```yaml
http_connector.{prefix}_{attribute}_jsonpath: "$.path.to.data"
http_connector.{prefix}_{attribute}_regex: "regex_pattern"
```

Where:
- `{prefix}` is either "principal" for user data or "principal_attribute" for user attributes
- `{attribute}` is the name of the attribute in Moat's data model

## Usage Examples

### Basic Configuration Example

Here's a basic configuration example for connecting to an API with API key authentication:

```yaml
http_connector.auth_method: "api-key"
http_connector.api_key: "your-api-key"
http_connector.url: "https://api.example.com/users"
http_connector.ssl_verify: true
```

### Mapping User Data

To map user data from the API response to Moat's `PrincipalDio` model:

```yaml
# Map user data fields
http_connector.principal_fq_name_jsonpath: "$.attributes.loginID"
http_connector.principal_first_name_jsonpath: "$.attributes.givenName"
http_connector.principal_last_name_jsonpath: "$.attributes.familyName"
http_connector.principal_user_name_jsonpath: "$.attributes.loginID"
http_connector.principal_email_jsonpath: "$.attributes.email"
```

### Mapping User Attributes

To map user attributes from the API response:

```yaml
# Map user attributes
http_connector.principal_attribute_fq_name_jsonpath: "$.attributes.loginID"
http_connector.principal_attribute_attributes_multi_jsonpath: "$.attributes.group[?(@ =~ '(?P<key>[a-zA-Z]+) (?P<value>[a-zA-Z0-9 ]+)')]"
http_connector.principal_attribute_attributes_multi_regex: "(?P<key>[a-zA-Z]+) (?P<value>[a-zA-Z0-9 ]+)"
```

This example extracts group memberships from the `group` array and parses them into key-value pairs using regex.

## Advanced Usage

### Using JSONPath Expressions

JSONPath expressions allow you to extract data from nested JSON structures:

- `$.name` - Access a top-level property
- `$.user.profile.email` - Access a nested property
- `$.groups[0].name` - Access an array element
- `$.groups[*].name` - Access all elements in an array

### Using Regex Patterns

Regex patterns can be used to:

1. Extract specific parts of a string
2. Transform data into the required format
3. Filter data based on patterns

Example:

```yaml
# Extract username from email address
http_connector.principal_user_name_jsonpath: "$.attributes.email"
http_connector.principal_user_name_regex: "^([^@]+)@.*$"
```

### Complete Example

Here's a complete example configuration for ingesting users from an identity provider API:

```yaml
# Authentication
http_connector.auth_method: "oauth2"
http_connector.oauth2_endpoint: "https://idp.example.com/oauth2/token"
http_connector.oauth2_client_id: "client-id"
http_connector.oauth2_client_secret: "client-secret"
http_connector.oauth2_grant_type: "client_credentials"
http_connector.oauth2_scope: "read:users"
http_connector.url: "https://idp.example.com/api/v1/users"

# User data mapping
http_connector.principal_fq_name_jsonpath: "$.attributes.loginID"
http_connector.principal_first_name_jsonpath: "$.attributes.givenName"
http_connector.principal_last_name_jsonpath: "$.attributes.familyName"
http_connector.principal_user_name_jsonpath: "$.attributes.loginID"
http_connector.principal_email_jsonpath: "$.attributes.email"

# User attributes mapping
http_connector.principal_attribute_fq_name_jsonpath: "$.attributes.loginID"
http_connector.principal_attribute_attributes_multi_jsonpath: "$.attributes.group[?(@ =~ '(?P<key>[a-zA-Z]+) (?P<value>[a-zA-Z0-9 ]+)')]"
http_connector.principal_attribute_attributes_multi_regex: "(?P<key>[a-zA-Z]+) (?P<value>[a-zA-Z0-9 ]+)"
```



## Troubleshooting

- **Authentication Errors**: Verify your authentication credentials and method
- **Data Mapping Issues**: Check your JSONPath expressions against the actual API response structure
- **Regex Pattern Problems**: Test your regex patterns with sample data from the API
- **SSL Certificate Errors**: Set `ssl_verify: false` for testing (not recommended for production) or provide the correct certificate path