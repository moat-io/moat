"""policy_evaluator_results

Revision ID: 037e41dc2b6c
Revises: 180d97518271
Create Date: 2026-10-22 10:06:10.809315+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

import database.src.datetime_utc


# revision identifiers, used by Alembic.
revision: str = '037e41dc2b6c'
down_revision: Union[str, Sequence[str], None] = '180d97518271'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE_NAME = "policy_evaluator_results"


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        TABLE_NAME,
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("principal_fq_name", sa.String(length=512), nullable=True),
        sa.Column("resource_fq_name", sa.String(length=512), nullable=True),
        sa.Column("platform", sa.String(length=100), nullable=True),
        sa.Column("action", sa.String(length=100), nullable=True),
        sa.Column(
            "active", sa.Boolean(), server_default=sa.text("true"), nullable=True
        ),
        sa.Column(
            "record_updated_date",
            database.src.datetime_utc.DateTimeUTC(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "record_created_date",
            database.src.datetime_utc.DateTimeUTC(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    bind = op.get_bind()

    # The record_updated_date trigger is Postgres-only. The set_record_updated_date()
    # function is created by migration 180d97518271; here we only attach the trigger
    # to the new table so Postgres stamps the date on every update.
    if bind.dialect.name != "postgresql":
        return

    op.execute(
        f"""
        CREATE OR REPLACE TRIGGER set_{TABLE_NAME}_record_updated_date
        BEFORE UPDATE ON {TABLE_NAME}
        FOR EACH ROW
        EXECUTE FUNCTION set_record_updated_date();
        """
    )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()

    if bind.dialect.name == "postgresql":
        op.execute(
            f"DROP TRIGGER IF EXISTS set_{TABLE_NAME}_record_updated_date ON {TABLE_NAME};"
        )

    op.drop_table(TABLE_NAME)
