import urllib3
from typing import Any, Generator

from app_logger import Logger, get_logger
from trino.dbapi import Cursor, connect
from trino.auth import BasicAuthentication, JWTAuthentication

from .trino_client_config import TrinoClientConfig

logger: Logger = get_logger("clients.trino_client")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class TrinoClient:
    trino_connection = None

    def __init__(self):
        config: TrinoClientConfig = TrinoClientConfig.load()

        if config.jwt_token:
            auth = JWTAuthentication(token=config.jwt_token)
        elif config.password:
            auth = BasicAuthentication(
                username=config.username, password=config.password
            )
        else:
            auth = None

        self.trino_connection = connect(
            host=config.host,
            port=config.port,
            user=config.username,
            auth=auth,
            http_scheme=config.http_scheme,
            verify=config.ssl_verify,
        )

    def select_async(
        self, query: str, batch_size: int = 5000
    ) -> Generator[list[dict[str, Any]], Any, None]:
        cursor: Cursor = self._get_cursor()
        self._execute(cursor=cursor, query=query)
        logger.info(f"Executing query {query} with id: {cursor.query_id}")

        schema: list[str] = self._get_schema(cursor=cursor)

        # Process each batch of results
        while True:
            batch = self._fetchmany(cursor=cursor, batch_size=batch_size)
            logger.info(
                f"Retrieved {len(batch)} records with query id: {cursor.query_id}"
            )
            if len(batch) == 0:
                break

            # a single column record or row gives us a stupid result
            if not isinstance(batch[0], list):
                batch = [batch]

            dict_records: list[dict[str, Any]] = []
            for record in batch:
                dict_records.append(dict(zip(schema, record)))
            yield dict_records

    def _get_cursor(self) -> Cursor:
        return self.trino_connection.cursor()

    def _execute(self, cursor: Cursor, query: str) -> None:
        cursor.execute(query)

    def _fetchmany(self, cursor: Cursor, batch_size: int) -> list[list]:
        return cursor.fetchmany(size=batch_size)

    def _get_schema(self, cursor: Cursor) -> list[str]:
        return [c.name for c in cursor.description]
