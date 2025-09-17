# Deployment
It is recommended to deploy `moat` in a containerised environment (e.g Kubernetes). A moat deployment consists of
an api server and a number of cronjobs.

## Database
Moat relies on a postgres database, it can be created with `psql` with the following script

```sql
create user moat_user with password '<password>>';
CREATE DATABASE moat;
GRANT ALL PRIVILEGES ON DATABASE moat TO moat_user;
\c moat;
GRANT ALL PRIVILEGES ON SCHEMA public TO moat_user;
```

## API Server
The API server should be deployed using a k8s `kind: Deployment` with optional replication. The API service is not required to be
highly-available so a single replica is fine for most purposes.

It is highly recommended that moat be deployed behind an SSL enabled load balancer or reverse proxy

## Ingestion Cronjobs
Data in moat is ingested and maintained by periodic ingestion jobs. These can be implemented in many ways however k8s `kind: CronJob`
are the recommended pattern. Any scheduler (crontab, airflow, CICD etc) can be used to execute the ingestion jobs via the CLI or container.

In general, four ingestion jobs are required, and can run at different frequencies. The individual jobs ingest:
* Principals (e.g users from a LDAP source)
* Principal attributes (e.g user tags from IdentityNow or other IDP)
* Resources (e.g tables or collections from a database)
* Resource attributes (e.g table tags from a database or data catalog)

## OPA and Trino
The OPA instance should be deployed as "close" as possible to the Trino coordinator. The API between Trino and OPA is heavily
used, so eliminating network hops is cruical for performance. Ideally the OPA container should be in the same pod, or at least
on the name node as the coordinator

## Deploying `moat` in Kubernetes

The repository includes example Kubernetes manifests that can be used as a starting point for deploying `moat` in a Kubernetes environment. 
**These templates are examples only** and should be customized to fit your specific requirements and environment.

### Example Kubernetes Manifests

The following Kubernetes manifest templates are provided:

#### Server Components
- `kubernetes/server/deployment.yaml`: Deployment for the moat-api server
- `kubernetes/server/service.yaml`: Service to expose the moat-api
- `kubernetes/server/configmap.yaml`: ConfigMap for Rego policies

#### Ingestor Components
- `kubernetes/ingestor/cronjob.yaml`: CronJob for scheduled data ingestion
- `kubernetes/ingestor/configmap.yaml`: ConfigMap for ingestor configuration

### Deployment Steps

1. Customize the configuration files:
   - Update the Rego policies in `configmap.yaml` with your actual authorization policies
   - Create appropriate secrets for database connections and other sensitive information
   - Modify the ingestor configuration to match your data sources

2. Apply the Kubernetes manifests:
```bash
kubectl apply -f kubernetes/server/
kubectl apply -f kubernetes/ingestor/
```

### Important Considerations

- **Resource Requirements**: Adjust CPU and memory requests/limits based on your workload
- **Scaling**: Consider setting up horizontal pod autoscaling for the API server
- **Persistence**: Configure appropriate persistent storage for any stateful components
- **Security**: Review and enhance the security settings, especially for production deployments
- **Monitoring**: Set up monitoring and alerting for the deployed components

Remember that these templates are starting points and should be adapted to your specific infrastructure, security requirements, and operational practices.
