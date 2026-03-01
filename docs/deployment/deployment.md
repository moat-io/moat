# Deployment
It is recommended to deploy `moat` in a containerised environment (e.g Kubernetes). A moat deployment consists of
an api server, a worker and a number of `cronjobs`.

# TODO
* update k8s manifests

## Database
Moat supports both `postgres` and `mysql` as backend databases. The database user supplied to moat should not be the 
root user, as this is not recommended. The database and user therefore should be created in advance of deploying the app.

### Postgres
The database should be created and populated with the following commands:
```sql
CREATE USER moat_user WITH PASSWORD '<password>>';
CREATE DATABASE moat;
GRANT ALL PRIVILEGES ON DATABASE moat TO moat_user;
\c moat;
GRANT ALL PRIVILEGES ON SCHEMA public TO moat_user;
```

### MySQL
The database should be created and populated with the following commands:
```sql
CREATE USER moat_user@localhost IDENTIFIED BY '<password>';
CREATE DATABASE moat;
GRANT ALL PRIVILEGES ON moat.* TO moat_user@localhost;
FLUSH PRIVILEGES;
```

## Web Server
The Web server should be deployed using a k8s `kind: Deployment` with optional replication. The API service is not required to be
highly-available so a single replica is fine for most purposes.

It is highly recommended that moat be deployed behind an SSL enabled load balancer or reverse proxy

### Starting the API server
The api server runs under `uwsgi` it can be started with the following command

```bash
./entrypoint.sh start-server
```

## Worker
The worker should be deployed as a long-lived standalone process. It executes tasks asynchronously, such as regenerating bundles as required.
In a kubernetes installation, it can be a second container in the same deployment as the API server, or a seperate deployment.

The primary role of the worker is to generate new bundles when metadata changes. The bundles are served by the API container,
so the worker and API containers need to share the same filesystem. In a kubernetes deployment, the worker and API containers
should be in the same pod with a shared `emptyDir` volume.

The worker entrypoint is a CLI command, similar to the ingestion processes. It is started with:
```bash
./entrypoint.sh start-worker
```

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

The following Kubernetes manifest templates are provided in the `/kubernetes` directory:

- **`namespace.yaml`**: Creates the `moat` namespace for deploying all Moat resources.

- **`deployment.yaml`**: Main application deployment with:
  - Init container that runs database migrations (`alembic upgrade head`)
  - Server container running the API server (`start-server`)
  - Worker container for background processing (`start-worker`)
  - Shared `bundle-storage` emptyDir volume mounted at `/opt/moat/bundles` for OPA bundle sharing
  - ConfigMap mounts for application config and Trino policy files

- **`service.yaml`**: ClusterIP Service exposing the API server on port 8000 within the cluster.

- **`secret.yaml`**: Contains sensitive environment variables including database credentials and secret keys. **Update with actual values before deployment.**

- **`configmap.yaml`**: Application configuration file mount (`moat-api-config`). **Populate with actual configuration from `moat/config/config.docker.yaml`.**

- **`trino-policy-configmap.yaml`**: ConfigMap for Trino-specific OPA policy files (`moat-config-trino-policy`). Add your Rego policy documents here.

- **`principal-ingestion-cronjob.yaml`**: CronJob that runs hourly to ingest principals (users/groups) from identity providers using the HTTP connector.

- **`resource-ingestion-cronjob.yaml`**: CronJob that runs hourly to ingest resources (databases/tables/columns) from Trino using the DBAPI connector.



### Deployment Steps

1. Customize the configuration files:
   - Update `secret.yaml` with your database credentials and secret keys
   - Populate `configmap.yaml` with your actual configuration (use `moat/config/config.docker.yaml` as a template)
   - Update the Rego policies in `trino-policy-configmap.yaml` with your actual authorization policies
   - Update image versions to your desired version

2. Deploy using Kustomize (recommended):
```bash
kubectl apply -k kubernetes/
```

   Or apply the manifests individually:
```bash
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/secret.yaml
kubectl apply -f kubernetes/configmap.yaml
kubectl apply -f kubernetes/trino-policy-configmap.yaml
kubectl apply -f kubernetes/deployment.yaml
kubectl apply -f kubernetes/service.yaml
kubectl apply -f kubernetes/principal-ingestion-cronjob.yaml
kubectl apply -f kubernetes/resource-ingestion-cronjob.yaml
```

3. Verify the deployment:
```bash
kubectl get pods -n moat
```

### Important Considerations

- **Resource Requirements**: Adjust CPU and memory requests/limits based on your workload
- **Scaling**: Consider setting up horizontal pod autoscaling for the API server
- **Persistence**: Configure appropriate persistent storage for any stateful components
- **Security**: Review and enhance the security settings, especially for production deployments
- **Monitoring**: Set up monitoring and alerting for the deployed components

Remember that these templates are starting points and should be adapted to your specific infrastructure, security requirements, and operational practices.
