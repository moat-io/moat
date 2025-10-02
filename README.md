<center>
<img src="docs/images/logo-moat.png" width="350" height="350">
</center>

# Moat :: The OPA bundle provider for Trino

## Documentation
https://moat-io.github.io/moat/

OPA and Trino are an awesome combination, but maintaining the policy documents and required data object
can be painful. Moat makes this easy with managed curation of principals and tables,
as well as a predefined set of ABAC policies suitable for most uses

Moat provides an API to serve bundles to OPA, including:
* Data objects and attributes ingested from various sources (SQL DBs,data catalogs etc)
* Principals and attributes/groups ingested from identity providers (SQL DB, LDAP, etc)
* Pre-built `rego` policy documents to support common use cases (e.g. RBAC) 

Moat itself is not involved in policy decisions at runtime, it  simply provides the information to the battle-hardened
OPA. Therefore moat is not required to have a high uptime, as downtime will not affect consumers of your trino instance.

Moat can serve bundles to any number of OPA/Trino installations. This makes it very convenient to manage permissions
across a fleet of trino clusters as well as ephemeral clusters. Simply add an OPA container to the coordinator deployment and 
point its bundle service to the moat deployment

## Features
* Bundle API for serving data and policy documents to OPA
* Extensible ingestion framework to acquire tables and users from external sources (e.g trino, ldap)
* Decision log capture

## Contributing
Contributions to Moat big or small, or just fixes to docs are always welcomed. See [contributing](CONTRIBUTING.md) for
details.

## Development
The Moat dev environment relies on a postgres database, OPA, trino, hive-metastore, minio and lldap to run.
All of these dependencies are present in the `docker-compose` in the repo root

[developer_setup.md](docs/developer_setup.md)

It is recommended to run the dependencies under `docker-compose` and debug the python code in your favourite editor 


#### Ingestion
Data ingestion is treated as first-class ETL in Moat
* Ingestion jobs are asynchronous and CLI-driven. Best deployed using kubernetes CronJobs or similar
* Common ingestion controller instantiates connectors specific to the ingestion source. 
  * Additional sources can easily be added by creating new connectors. Currently supports Trino and LDAP
  * Connectors are python classes based on `ConnectorBase` and yield instances of common ingestion objects
  * Connectors instantiate specific client classes, depending on the use-case. (E.g: LDAP connector instantiates an LDAP client)
* Repository model provides a common `SQL MERGE` tool to provide robust transaction oriented ingestion

### Related Reading
* https://trino.io/docs/current/security/opa-access-control.html
* https://www.openpolicyagent.org/
