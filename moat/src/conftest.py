import os
from typing import Any, Generator

import pytest
from flask import Flask
from flask.testing import FlaskClient
from pytest_postgresql.janitor import DatabaseJanitor
from alembic import command
from alembic.config import Config
from sqlalchemy.sql import text

os.environ["CONFIG_FILE_PATH"] = "moat/config/config.unittest.yaml"
os.environ["FLASK_SECRET_KEY"] = "dont-tell-anyone"
os.environ["FLASK_TESTING"] = "true"

from app import create_app
from database import Database
from database.src.database_seeder import DatabaseSeeder


@pytest.fixture(scope="module")
def database_empty() -> Generator[Database, Any, None]:
    db: Database = Database()
    with DatabaseJanitor(
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
def database_warehouse() -> Generator[Database, Any, None]:
    db: Database = Database()
    db.config.database = "warehouse"

    with DatabaseJanitor(
        user=db.config.user,
        password=db.config.password,
        host=db.config.host,
        port=db.config.port,
        dbname="warehouse",
        version=16,
        connection_timeout=2,
    ):
        db.connect()

        # drop all users who start with "unittest_"
        with db.Session.begin() as session:
            result = session.execute(
                text(
                    "select rolname from pg_roles "
                    "where rolname not in "
                    "('hive_user', 'keycloak_user', 'postgres', 'test_user', 'trino_user')"
                    "and rolname not like 'pg_%';"
                )
            ).all()
            if len(result) > 0:
                session.execute(
                    text(f"DROP USER IF EXISTS {','.join([r[0] for r in result])};")
                )

            session.execute(text("CREATE SCHEMA sales;"))
            session.execute(
                text(
                    """CREATE TABLE sales.customer_markets
                       (
                           id            VARCHAR,
                           type_name     VARCHAR,
                           contact_name  VARCHAR,
                           contact_title VARCHAR,
                           address       VARCHAR,
                           city          VARCHAR,
                           region        VARCHAR,
                           postal_code   VARCHAR,
                           country       VARCHAR,
                           phone         VARCHAR
                       );
                    """
                )
            )

            session.execute(
                text(
                    """CREATE TABLE sales.customers
                        (
                            id            VARCHAR,
                            company_name  VARCHAR,
                            contact_name  VARCHAR,
                            contact_title VARCHAR,
                            address       VARCHAR,
                            city          VARCHAR,
                            region        VARCHAR,
                            postal_code   VARCHAR,
                            country       VARCHAR,
                            phone         VARCHAR
                       );
                    """
                )
            )
            session.commit()
        yield db


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
