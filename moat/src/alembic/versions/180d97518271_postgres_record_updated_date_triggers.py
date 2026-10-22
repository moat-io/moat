"""postgres record_updated_date triggers

Revision ID: 180d97518271
Revises: e1833e19df8b
Create Date: 2026-08-01 22:06:32.653786+00:00

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "180d97518271"
down_revision: Union[str, Sequence[str], None] = "e1833e19df8b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Tables whose SQLAlchemy definition inherits from ``MetadataDboMixin``
# (see moat/src/models/src/dbos/common_mixin_dbo.py). The mixin declares
# ``record_updated_date`` with ``server_onupdate=func.now()``, but that is
# informational only in SQLAlchemy and does not create a database-side
# trigger. This migration adds a BEFORE UPDATE trigger so Postgres stamps
# ``record_updated_date`` on every row update regardless of the writer.
METADATA_TABLES = (
    "principals",
    "principal_groups",
    "principal_attributes",
    "resources",
    "resource_attributes",
    "opa_bundles",
)


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()

    # Postgres-only migration; no-op on any other dialect.
    if bind.dialect.name != "postgresql":
        return

    op.execute(
        """
        CREATE OR REPLACE FUNCTION set_record_updated_date()
            RETURNS TRIGGER
            LANGUAGE PLPGSQL
        AS $$
        BEGIN
            NEW.record_updated_date = now();
            RETURN NEW;
        END;
        $$;
        """
    )

    for table in METADATA_TABLES:
        op.execute(
            f"""
            CREATE OR REPLACE TRIGGER set_{table}_record_updated_date
            BEFORE UPDATE ON {table}
            FOR EACH ROW
            EXECUTE FUNCTION set_record_updated_date();
            """
        )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()

    if bind.dialect.name != "postgresql":
        return

    for table in METADATA_TABLES:
        op.execute(
            f"DROP TRIGGER IF EXISTS set_{table}_record_updated_date ON {table};"
        )

    op.execute("DROP FUNCTION IF EXISTS set_record_updated_date();")
