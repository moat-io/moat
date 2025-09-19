from database import Database
from models import *
from alembic import command
from alembic.config import Config
from sqlalchemy.sql import text


def create_db():
    db: Database = Database()
    db.connect(echo_statements=True)
    db.drop_all_tables()

    with db.Session.begin() as session:
        session.execute(text("DROP TABLE IF EXISTS alembic_version"))

    alembic_cfg = Config("moat/alembic.ini")
    command.upgrade(alembic_cfg, "head")


if __name__ == "__main__":
    create_db()
