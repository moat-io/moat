# SCIM2.0
Moat provides a `SCIM2.0` compliant interface to make it easy for identity providers and other supported client types.

## Configuration
### Client Configuration
When connecting a client to moat's SCIM2.0 server, minimal configuration is required as a `/ServiceProviderConfig` 
endpoint has been provided, which allows auto-configuration of clients. 

The SCIM2.0 service is available at `/api/scim/v2/`

### Authentication
See [auth](../security/auth.md) for details of API authentication 

## Schemas
Moat provides two default SCIM2.0 schemas, User and Group. 

The default schema files are located at:

* `/moat/config/scim_user_schema.json`
* `/moat/config/scim_group_schema.json`

### urn:ietf:params:scim:schemas:core:2.0:User

| Field                  | Type   | Purpose                                               |
|------------------------|--------|-------------------------------------------------------|
| `id`                   | string | User ID in source system. Defaults to UUID if not set |
| `userName`             | string | The username which will be provided to OPA / Trino    |
| `name.givenName`       | string | User's first name                                     |
| `name.familyName`      | string | User's last name                                      |
| `emails`               | array  |                                                       |
| `emails[].value`       | email  | Email address. Only primary is captured               |
| `emails[].primary`     | bool   | Primary email or other                                |
| `active`               | bool   | Whether the user is active or not                     |
| `entitlements`         | array  |                                                       |
| `entitlements[].value` | string | The user's entitlement                                |



### urn:ietf:params:scim:schemas:core:2.0:Group

| Field             | Type   | Purpose                                               |
|-------------------|--------|-------------------------------------------------------|
| `id`              | string | User ID in source system. Defaults to UUID if not set |
| `displayName`     | string | The group name from the source                        |
| `members`         | array  |                                                       |
| `members.value`   | string | Member username (Should match `User.userName`)        |
| `members.type`    | string | Member type `User` or `Group`                         |
| `members.$ref`    | string | The URI of this group                                 |
| `members.display` | string | Human readable name or description                    |


### Schema Customisation
The schemas are configurable to suit your specific use case. To use a custom schema, follow these steps:

#### 1. Define and mount custom schema file
Edit your schema file and store/mount it in the local filesystem, e.g `/opt/moat/scim/`

The schema file must be `JSON` format and be generally compliant with the SCIM2.0 protocol. Many examples are available

#### 2. Define `jsonpaths` to extract the required information
Moat's SCIM interface uses `jsonpath` to extract the required values from the SCIM payload which are needed for ingestion
into the database. See the `ScimConfig` class or [configuration](../config/properties.md) for details.

```
// schema
{
    "id": "urn:ietf:params:scim:schemas:core:2.0:User",
    "name": "User",
    "description": "User Account",
    "attributes": [
        {
            "name": "userName",
            ...
        },
        {
            "name": "email",
            ...
        },
    ]
}

// payload
{
    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
    "userName": "alice.cooper",
    "email": "alice.cooper@moat.io"
}

// config
scim.principal_fq_name_jsonpath: "$.userName"
scim.principal_email_jsonpath: "$.email"
```

#### 3. Run the SCIM test tool to check for compliance
Check [developer setup](../developer_setup.md) for details of the `scim2-cli` tool which can be used to test schemas

### Attribute extraction
As Moat is designed for both ABAC and RBAC usage, the SCIM interface supports attributes to be extracted from any part of the payload.

In this example, a custom schema is defined (a better name is suggested) and attributes are being extracted from it using the `jsonpath`
expression. All keys selected by the `jsonpath` are converted to key-value pairs and applied to the user.

Multi-value attributes are stored as comma-seperated text, but split into individual values by the bundler.

```
// payload
{
    "schemas": [
        "urn:ietf:params:scim:schemas:core:2.0:User",
        "urn:ietf:params:scim:custom",
    ],
    "userName": "alice.cooper",
    "email": "alice.cooper@moat.io"
    "urn:ietf:params:scim:custom": {
        "Employee": "True",
        "Redact": "PII",
        "Domain": ["Sales", "Customer", "HR"],
    },
}

// config
scim.principal_attributes_jsonpath = "$.'urn:ietf:params:scim:custom'"
```

**Extracted Attributes**

| Key          | Value             |
|--------------|-------------------|
| Employee     | True              |
| Redact       | PII               |
| Domain       | Sales,Customer,HR |



## Use by non-scim clients
A client does not need to implement the full SCIM protocol in order to interact with the SCIM APIs. Fully-compliant 
SCIM clients will use the metadata APIs (`/ResourceTypes`) etc, however simpler clients may prefer to directly use the
`/User` and `/Group` endpoints. Each of these endpoints provides a standard CRUD interface via REST.

