# How it works

## Overview
Moat provides the data required by OPA to make its policy decisions. 
OPA expects a bundle file (a tarball) comprised of:

* `rego` policy documents defining the policies to be applied
* A data object - a `json` file containing:
  * User metadata - e.g username, groups, attributes, tags
  * Resource (tables/views) metadata - e.g database, schema, table name and tags (usually from a data catalog)

OPA periodically requests this data via an HTTP(S) API.

### Metadata Classes
The metadata classes supported by moat are:

* **Principals:** Users and service accounts
* **Principal Attributes:** Key-value pairs describing users, normally from an identity provider
* **Principal Groups:** RBAC-style user groups
* **Resources:** Data objects such as databases, schemas, tables, columns, collections etc
* **Resource Attributes:** Key-value pairs as either attributes or tags on data objects. Normally from a data catalog

## User Metadata
User metadata can either be periodically ingested from an identity source, or pushed by a SCIM2.0 client.

### SCIM2.0
Moat implements the industry standard SCIM2.0 protocol to allow changes to users, groups and entitlements
to be received in near real time. The standard `User` and `Group` schemas are supported, and custom schemas
can be defined. See [scim](provisioning/scim.md) for more information

### Ingestion
For implementations where a SCIM client is not available or desired, users, groups and entitlements can be 
ingested from various sources. Currently Moat supports `LDAP` as a source of users and groups.

The ingestion system is easy to extend by providing a subclass containing the specific implementation of your
chosen identity source. See [extensions](extensions.md) for more information on custom connectors.

## Object Metadata
Object metadata is ingested in the same way as users and groups. Currently moat supports SQL sources via DBAPI 
connectors to ingest data objects and their attributes or tags. Custom connectors can also be built for sources other 
than those supported OOTB. Contributions are most welcome for new connectors.

## Policy Documents
Policy documents are not ingested by moat as such, but are included in the bundle provided to trino.

It is recommended that policy documents be controlled using normal version control tooling (.e.g git).
The `rego` files can be provided to moat using either a `git-sync` pattern, or any normal CICD process.

Configmaps are a good pattern for kubernetes-based deployments

## Bundles
See [Bundles](bundles.md)