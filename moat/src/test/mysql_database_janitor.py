import pymysql


class MysqlDatabaseJanitor:
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: str,
        dbname: str,
        version: int = 0,
        connection_timeout: int = 0,
    ) -> None:
        self.user = user
        self.password = password
        self.host = host
        self.port = int(port)
        self.dbname = dbname

    def __enter__(self):
        """
        Connects to MySQL database
        Checks if the specified DB exists
        Drops the DB if it exists
        Creates the DB
        """
        connection = self._get_connection()
        self._drop_db(connection)
        self._create_db(connection)
        connection.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        connection = self._get_connection()
        self._drop_db(connection)
        connection.close()

    def _get_connection(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
        )

    def _drop_db(self, connection: pymysql.connections.Connection) -> None:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {self.dbname}")

    def _create_db(self, connection: pymysql.connections.Connection) -> None:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {self.dbname}")
            cursor.execute(f"CREATE DATABASE {self.dbname}")
