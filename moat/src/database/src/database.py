from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from .database_config import DatabaseConfig
from app_logger import Logger, get_logger

BaseModel = declarative_base()

logger: Logger = get_logger("database")



class Database:
    """
    Usage:

    database: Database = Database()     # no config required
    database.Session.begin() as session:
        session.add(thing)
    # commit is automatic when exiting the with block
    """

    engine: Engine
    Session: sessionmaker
    config: DatabaseConfig

    def __init__(self):
        self.config: DatabaseConfig = DatabaseConfig.load()

    def create_engine(self, echo_statements: bool = False) -> Engine:
        logger.info(f"Connecting to database at={self.config.host}:{self.config.port}/{self.config.database}")
        engine: Engine = create_engine(
            self.config.connection_string,
            echo=echo_statements,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        return engine

    def connect(self, echo_statements: bool = False) -> None:
        self.engine: Engine = self.create_engine(
            echo_statements=echo_statements,
        )
        self.Session = sessionmaker(self.engine)

    def create_all_tables(self):
        logger.info("Creating all database tables")
        BaseModel.metadata.create_all(self.engine)

    def drop_all_tables(self):
        logger.info("Dropping all database tables")
        BaseModel.metadata.drop_all(self.engine)

    def disconnect(self) -> None:
        logger.info("Disconnecting from database")
        self.engine.dispose()
