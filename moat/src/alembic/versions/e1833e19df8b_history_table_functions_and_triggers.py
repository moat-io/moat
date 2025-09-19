"""history table functions and triggers

Revision ID: e1833e19df8b
Revises: e73c0de18f01
Create Date: 2025-09-19 06:34:03.483939+00:00

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e1833e19df8b"
down_revision: Union[str, Sequence[str], None] = "e73c0de18f01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Function and trigger for resource_attributes_history
    op.execute(
        """
        CREATE
        OR REPLACE FUNCTION record_history()
                  RETURNS TRIGGER
                  LANGUAGE PLPGSQL
                  AS
                $$
        BEGIN
                    IF
        (TG_OP = 'DELETE') THEN
            execute 'insert into ' || TG_TABLE_NAME || '_history select ($1).*, gen_random_uuid(), now(), ''D''' using old;
        ELSIF
        (TG_OP = 'UPDATE') THEN -- both OLD and NEW are valid
            execute 'insert into ' || TG_TABLE_NAME || '_history select ($1).*, gen_random_uuid(), now(), ''U''' using new;
        ELSIF
        (TG_OP = 'INSERT') THEN -- OLD is null, NEW is valid
            execute 'insert into ' || TG_TABLE_NAME || '_history select ($1).*, gen_random_uuid(), now(), ''I''' using new;
        END IF;
        RETURN NULL; -- not required for an AFTER trigger
        END;
                $$;
        """
    )

    # resources
    op.execute(
        """
        CREATE OR REPLACE TRIGGER record_resource_history
        AFTER INSERT OR UPDATE OR DELETE
        ON resources
        FOR EACH ROW EXECUTE FUNCTION record_history();
        """
    )

    # resource attributes
    op.execute(
        """
        CREATE OR REPLACE TRIGGER record_resource_attributes_history
        AFTER INSERT OR UPDATE OR DELETE
        ON resource_attributes
        FOR EACH ROW EXECUTE FUNCTION record_history();
        """
    )

    # principals
    op.execute(
        """
        CREATE
        OR REPLACE TRIGGER record_principal_history
            AFTER INSERT OR
        UPDATE OR
        DELETE
        ON principals
            FOR EACH ROW EXECUTE FUNCTION record_history();
        """
    )

    # principal attributes
    op.execute(
        """
        CREATE
        OR REPLACE TRIGGER record_principal_attributes_history
            AFTER INSERT OR
        UPDATE OR
        DELETE
        ON principal_attributes
            FOR EACH ROW EXECUTE FUNCTION record_history();
        """
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP TRIGGER IF EXISTS record_resource_history on resources;")
    op.execute(
        "DROP TRIGGER IF EXISTS record_resource_attributes_history on resource_attributes;"
    )
    op.execute("DROP TRIGGER IF EXISTS record_principal_history on principals;")
    op.execute(
        "DROP TRIGGER IF EXISTS record_principal_attributes_history on principal_attributes;"
    )
    op.execute("DROP FUNCTION IF EXISTS record_resource_attributes_history();")
