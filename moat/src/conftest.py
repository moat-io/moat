import os
from typing import Any, Generator

import pytest
from test import MysqlDatabaseJanitor
from flask import Flask
from flask.testing import FlaskClient
from pytest_postgresql.janitor import DatabaseJanitor as PostgresDatabaseJanitor
from alembic import command
from alembic.config import Config

os.environ["CONFIG_FILE_PATH"] = os.getenv(
    "CONFIG_FILE_PATH", "moat/config/config.unittest.yaml"
)
os.environ["FLASK_SECRET_KEY"] = "dont-tell-anyone"
os.environ["FLASK_TESTING"] = "true"

from app import create_app
from database import Database
from database.src.database_seeder import DatabaseSeeder


@pytest.fixture(scope="module")
def database_empty() -> Generator[Database, Any, None]:
    db: Database = Database()

    if "postgresql" in db.config.protocol:
        janitor_class = PostgresDatabaseJanitor
    elif "mysql" in db.config.protocol:
        janitor_class = MysqlDatabaseJanitor
    else:
        raise ValueError(f"Unsupported database protocol: {db.config.protocol}")

    with janitor_class(
        user=db.config.user,
        password=db.config.password,
        host=db.config.host,
        port=db.config.port,
        dbname=db.config.database,
        version=16,
        connection_timeout=2,
    ):
        db.connect()
        # Run all alembic migrations instead of creating tables directly
        alembic_cfg = Config("moat/alembic.ini")
        command.upgrade(alembic_cfg, "head")
        yield db


@pytest.fixture(scope="module")
def database(database_empty: Database) -> Generator[Database, Any, None]:
    database_seeder: DatabaseSeeder = DatabaseSeeder(db=database_empty)
    database_seeder.seed()
    yield database_empty


@pytest.fixture(scope="module")
def flask_app(database_empty: Database) -> Generator[Flask, Any, None]:
    app = create_app(database=database_empty)
    app.config.update(
        {
            "TESTING": True,
        }
    )
    yield app


@pytest.fixture(scope="module")
def flask_test_client(flask_app: Flask) -> FlaskClient:
    return flask_app.test_client()
