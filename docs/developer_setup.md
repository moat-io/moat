# Developer Setup

## Application Components
* **API server** is very lightweight and is based on `flask`
* **Unit tests** are under `pytest`
* **ORM** is SQLAlchemy with postgres16, unit tests use `DatabaseJanitor` with dockerised postgres

### Installation of dependencies
* Virtual environment should be at the project root
* Requires local installation of OPA and python 3.12
* Supporting apps run under docker-compose
* An SQL client (e.g: dbeaver) is also required

```bash
brew install opa
brew install python@3.12

python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

```

## Code Formatting
```bash
# in project root
isort --profile black moat/src
black moat/src
```
## Running the dev environment
* Run flask on 8000

### Starting the dev server
```bash
# if using podman
podman machine init
podman machine start
podman-compose up -d

# run dependencies
podman-compose up -d

# populate LLDAP:
podman-compose exec lldap /bootstrap/bootstrap.sh

export PYTHONPATH=moat/src
flask --app moat.src.app run --debug --port 8000

# seed the database
python moat/moat/src/seed_db.py

# nuking a bad flask process
kill $(pgrep -f flask)

# all the data for lldap and postgres is stored in the `instance` dir. nuke it to reset the app
rm -rf instance
```

### Running the container
```bash
podman run --rm -p 3000:8000 moat/moat:0.0.1
curl localhost:3000/api/v1/healthcheck
```

### Running with Docker Compose
To run the Moat application along with its dependencies (OPA, PostgreSQL, etc.), you can add the following service to your `docker-compose.yaml` file:

```yaml
services:

  moat:
    build: .
    ports:
      - "8000:8000"
    environment:
      - FLASK_SECRET_KEY=your_secret_key
      - OIDC_AUTH_PROVIDER_CLIENT_SECRET=your_client_secret
    depends_on:
      - opa
      - postgres
```

## Building the container image
```bash
docker build -t moat/moat
```

## Running Ingestion
```bash
export CONFIG_FILE_PATH=moat/config.principal_ingestion.yaml
cli.py ingest --source=ldap --object-type=principal

# or in the container
docker run moat ingest --connector-name=ldap --object-type=principal
```

## Database Migrations
`alembic` is used to migrate the database schema

```bash
# create a new revision (in project root)
export PYTHONPATH=./moat/src
export CONFIG_FILE_PATH=moat/config/config.yaml
alembic -c moat/alembic.ini revision --autogenerate -m "message about the revision"


# upgrade
alembic -c moat/alembic.ini upgrade head
```

## Documentation
Docs are provided by `mkdocs` using the `mkdocs-material` theme.
The documentation is automatically deployed to GitHub Pages via GitHub Actions when changes are pushed to the main branch.

For local development:
```bash
# render docs for development
mkdocs serve
```

Manual deployment (not needed for normal workflow as it's automated):
```bash
# deploy docs manually
mkdocs gh-deploy

# if it fails, try:
git config http.postBuffer 524288000
```


## Keycloak
### Export Realm
```bash
podman-compose up -d
podman-compose run keycloak export --file /opt/keycloak/data/export/moat_realm.json --realm moat
```

### Testing Oauth2
```yaml
# config.yaml setup:
api.healthcheck.auth_method: oauth2
api.healthcheck.oauth2_issuer: http://localhost:8080/realms/moat
api.healthcheck.oauth2_audience: account
api.healthcheck.oauth2_jwks_uri: http://localhost:8080/realms/moat/protocol/openid-connect/certs
api.healthcheck.oauth2_read_scope: profile    # change this to test scope access
```

```bash
# should pass
python moat/src/_scripts/api_oauth2_test_script.py

# change config & restart app
api.healthcheck.oauth2_read_scope: profile-bad

# should fail
python moat/src/_scripts/api_oauth2_test_script.py
```

## SCIM test tool
```bash
pip install scim2-cli

./test.sh scim
```