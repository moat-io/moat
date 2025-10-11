from .grant_connector_base import GrantConnectorBase


class TestConnector(GrantConnectorBase):
    def __init__(self):
        super().__init__()
        self.principals = {}

    def get_config(self):
        return None

    def sync_principals(self, usernames: list[str]):
        for username in usernames:
            if username not in self.principals:
                self.principals[username] = {"tables": []}

    def sync_grants(self, username: str, table: str, grant: bool):
        if username not in self.principals.keys():
            raise ValueError(f"Principal {username} not found")

        if grant and table not in self.principals[username]["tables"]:
            self.principals[username]["tables"].append(table)

        if not grant and table in self.principals[username]["tables"]:
            self.principals[username]["tables"].remove(table)
