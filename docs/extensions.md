# Extending Moat

Moat is designed to be extended in various ways to meet your specific needs.

## Custom Ingestion

Moat allows you to create custom ingestion connectors to import data from various sources. This is done by creating subclasses of the `ConnectorBase` class.
Subclasses have a `CONNECTOR_NAME` property which allows the moat to select it at runtime 

### Creating a Custom Connector

To create a custom connector, follow these steps:

1. Create a new Python module for your connector
2. Create a class that inherits from `ConnectorBase`
3. Implement the required methods:
   - `acquire_data(platform)`: Fetch data from your source
   - `get_principals()`: Transform your data into `PrincipalDio` objects
   - `get_principal_attributes()` (optional): Extract attributes for principals

### Example: JSON File Connector

Here's an example of a connector that loads users from a local JSON file:

```python
import json
from app_logger import Logger, get_logger
from app_config import AppConfigModelBase
from ingestor.connectors.connector_base import ConnectorBase
from ingestor.models import PrincipalDio, PrincipalAttributeDio

logger: Logger = get_logger("ingestor.connectors.json_file_connector")

class JsonFileConnectorConfig(AppConfigModelBase):
    CONFIG_PREFIX: str = "json_file_connector"
    json_file_path: str = "users.json"

class JsonFileConnector(ConnectorBase):
    CONNECTOR_NAME: str = "json-file"

    def __init__(self):
        super().__init__()
        self.config: JsonFileConnectorConfig = JsonFileConnectorConfig.load()
        self.users = []
        logger.info(f"Created JSON file connector using file: {self.config.json_file_path}")

    def acquire_data(self, platform: str) -> None:
        self.platform = platform
        with open(self.config.json_file_path, 'r') as file:
            self.users = json.load(file)
        logger.info(f"Retrieved {len(self.users)} users from JSON file")

    def get_principals(self) -> list[PrincipalDio]:
        principals: list[PrincipalDio] = []

        for user in self.users:
             principal = PrincipalDio(
                 fq_name=user.get("id"),
                 first_name=user.get("first_name"),
                 last_name=user.get("last_name"),
                 user_name=user.get("username"),
                 email=user.get("email"),
                 platform=self.platform,
             )
             principals.append(principal)

        return principals

    def get_principal_attributes(self) -> list[PrincipalAttributeDio]:
        attributes: list[PrincipalAttributeDio] = []

        for user in self.users:
             fq_name = user.get("id")

             # Process groups if they exist
             if "groups" in user:
                 for group in user["groups"]:
                     attribute = PrincipalAttributeDio(
                         fq_name=fq_name,
                         attribute_key="group",
                         attribute_value=group,
                         platform=self.platform,
                     )
                     attributes.append(attribute)

             # Process any other attributes
             if "attributes" in user:
                 for key, value in user["attributes"].items():
                     attribute = PrincipalAttributeDio(
                         fq_name=fq_name,
                         attribute_key=key,
                         attribute_value=value,
                         platform=self.platform,
                     )
                     attributes.append(attribute)

        return attributes
```

### Example JSON File Format

The connector above expects a JSON file with a structure like this:

```json
[
  {
    "id": "user1",
    "first_name": "John",
    "last_name": "Doe",
    "username": "johndoe",
    "email": "john.doe@example.com",
    "groups": ["admin", "users"],
    "attributes": {
      "department": "Engineering",
      "location": "New York"
    }
  },
  {
    "id": "user2",
    "first_name": "Jane",
    "last_name": "Smith",
    "username": "janesmith",
    "email": "jane.smith@example.com",
    "groups": ["users"],
    "attributes": {
      "department": "Marketing",
      "location": "San Francisco"
    }
  }
]
```

### Configuring Your Connector

The `JsonFileConnectorConfig` class uses the `AppConfigModelBase` to load configuration values from a YAML file. 
You can configure your connector by adding entries to your config file (typically at `moat/config/config.yaml`):

```yaml
# config files use the CONFIG_PREFIX (json_file_connector) in the config class to load the parameters
json_file_connector.json_file_path: /path/to/your/users.json
```

The configuration values are loaded automatically when the connector is initialized.

### Using Your Custom Connector

Once you've created your connector, you can invoke it from the CLI like any other connector:

```bash
export CONFIG_FILE_PATH=moat/config.custom_ingestion.yaml
cli.py ingest --connector-name=json-file --object-type=principal
cli.py ingest --connector-name=json-file --object-type=principal_attribute
```
