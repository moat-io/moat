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
down_revision: Union[str, Sequence[str], None] = "cd68c92b9b9c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()

    if bind.dialect.name == "postgresql":
        # PostgreSQL implementation using function and triggers
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

    elif bind.dialect.name == "mysql":
        # MySQL implementation - separate triggers for INSERT, UPDATE, DELETE on each table
        # MySQL doesn't support SELECT NEW.* so we need to explicitly list all columns

        # resources - INSERT
        op.execute(
            """
            CREATE TRIGGER record_resource_history_insert
            AFTER INSERT ON resources
            FOR EACH ROW
            INSERT INTO resources_history (
                id, fq_name, platform, object_type,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                NEW.id, NEW.fq_name, NEW.platform, NEW.object_type,
                NEW.ingestion_process_id, NEW.active,
                NEW.record_updated_date, NEW.record_created_date,
                UUID(), NOW(), 'I'
            );
            """
        )

        # resources - UPDATE
        op.execute(
            """
            CREATE TRIGGER record_resource_history_update
            AFTER UPDATE ON resources
            FOR EACH ROW
            INSERT INTO resources_history (
                id, fq_name, platform, object_type,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                NEW.id, NEW.fq_name, NEW.platform, NEW.object_type,
                NEW.ingestion_process_id, NEW.active,
                NEW.record_updated_date, NEW.record_created_date,
                UUID(), NOW(), 'U'
            );
            """
        )

        # resources - DELETE
        op.execute(
            """
            CREATE TRIGGER record_resource_history_delete
            AFTER DELETE ON resources
            FOR EACH ROW
            INSERT INTO resources_history (
                id, fq_name, platform, object_type,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                OLD.id, OLD.fq_name, OLD.platform, OLD.object_type,
                OLD.ingestion_process_id, OLD.active,
                OLD.record_updated_date, OLD.record_created_date,
                UUID(), NOW(), 'D'
            );
            """
        )

        # resource_attributes - INSERT
        op.execute(
            """
            CREATE TRIGGER record_resource_attributes_history_insert
            AFTER INSERT ON resource_attributes
            FOR EACH ROW
            INSERT INTO resource_attributes_history (
                id, fq_name, attribute_key, attribute_value,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                NEW.id, NEW.fq_name, NEW.attribute_key, NEW.attribute_value,
                NEW.ingestion_process_id, NEW.active,
                NEW.record_updated_date, NEW.record_created_date,
                UUID(), NOW(), 'I'
            );
            """
        )

        # resource_attributes - UPDATE
        op.execute(
            """
            CREATE TRIGGER record_resource_attributes_history_update
            AFTER UPDATE ON resource_attributes
            FOR EACH ROW
            INSERT INTO resource_attributes_history (
                id, fq_name, attribute_key, attribute_value,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                NEW.id, NEW.fq_name, NEW.attribute_key, NEW.attribute_value,
                NEW.ingestion_process_id, NEW.active,
                NEW.record_updated_date, NEW.record_created_date,
                UUID(), NOW(), 'U'
            );
            """
        )

        # resource_attributes - DELETE
        op.execute(
            """
            CREATE TRIGGER record_resource_attributes_history_delete
            AFTER DELETE ON resource_attributes
            FOR EACH ROW
            INSERT INTO resource_attributes_history (
                id, fq_name, attribute_key, attribute_value,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                OLD.id, OLD.fq_name, OLD.attribute_key, OLD.attribute_value,
                OLD.ingestion_process_id, OLD.active,
                OLD.record_updated_date, OLD.record_created_date,
                UUID(), NOW(), 'D'
            );
            """
        )

        # principals - INSERT
        op.execute(
            """
            CREATE TRIGGER record_principal_history_insert
            AFTER INSERT ON principals
            FOR EACH ROW
            INSERT INTO principals_history (
                principal_id, fq_name, first_name, last_name, user_name, email,
                source_type, source_uid, scim_payload, entitlements,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                NEW.principal_id, NEW.fq_name, NEW.first_name, NEW.last_name, NEW.user_name, NEW.email,
                NEW.source_type, NEW.source_uid, NEW.scim_payload, NEW.entitlements,
                NEW.ingestion_process_id, NEW.active,
                NEW.record_updated_date, NEW.record_created_date,
                UUID(), NOW(), 'I'
            );
            """
        )

        # principals - UPDATE
        op.execute(
            """
            CREATE TRIGGER record_principal_history_update
            AFTER UPDATE ON principals
            FOR EACH ROW
            INSERT INTO principals_history (
                principal_id, fq_name, first_name, last_name, user_name, email,
                source_type, source_uid, scim_payload, entitlements,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                NEW.principal_id, NEW.fq_name, NEW.first_name, NEW.last_name, NEW.user_name, NEW.email,
                NEW.source_type, NEW.source_uid, NEW.scim_payload, NEW.entitlements,
                NEW.ingestion_process_id, NEW.active,
                NEW.record_updated_date, NEW.record_created_date,
                UUID(), NOW(), 'U'
            );
            """
        )

        # principals - DELETE
        op.execute(
            """
            CREATE TRIGGER record_principal_history_delete
            AFTER DELETE ON principals
            FOR EACH ROW
            INSERT INTO principals_history (
                principal_id, fq_name, first_name, last_name, user_name, email,
                source_type, source_uid, scim_payload, entitlements,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                OLD.principal_id, OLD.fq_name, OLD.first_name, OLD.last_name, OLD.user_name, OLD.email,
                OLD.source_type, OLD.source_uid, OLD.scim_payload, OLD.entitlements,
                OLD.ingestion_process_id, OLD.active,
                OLD.record_updated_date, OLD.record_created_date,
                UUID(), NOW(), 'D'
            );
            """
        )

        # principal_attributes - INSERT
        op.execute(
            """
            CREATE TRIGGER record_principal_attributes_history_insert
            AFTER INSERT ON principal_attributes
            FOR EACH ROW
            INSERT INTO principal_attributes_history (
                principal_attribute_id, fq_name, attribute_key, attribute_value,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                NEW.principal_attribute_id, NEW.fq_name, NEW.attribute_key, NEW.attribute_value,
                NEW.ingestion_process_id, NEW.active,
                NEW.record_updated_date, NEW.record_created_date,
                UUID(), NOW(), 'I'
            );
            """
        )

        # principal_attributes - UPDATE
        op.execute(
            """
            CREATE TRIGGER record_principal_attributes_history_update
            AFTER UPDATE ON principal_attributes
            FOR EACH ROW
            INSERT INTO principal_attributes_history (
                principal_attribute_id, fq_name, attribute_key, attribute_value,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                NEW.principal_attribute_id, NEW.fq_name, NEW.attribute_key, NEW.attribute_value,
                NEW.ingestion_process_id, NEW.active,
                NEW.record_updated_date, NEW.record_created_date,
                UUID(), NOW(), 'U'
            );
            """
        )

        # principal_attributes - DELETE
        op.execute(
            """
            CREATE TRIGGER record_principal_attributes_history_delete
            AFTER DELETE ON principal_attributes
            FOR EACH ROW
            INSERT INTO principal_attributes_history (
                principal_attribute_id, fq_name, attribute_key, attribute_value,
                ingestion_process_id, active,
                record_updated_date, record_created_date,
                history_id, history_record_created_date, history_change_operation
            )
            VALUES (
                OLD.principal_attribute_id, OLD.fq_name, OLD.attribute_key, OLD.attribute_value,
                OLD.ingestion_process_id, OLD.active,
                OLD.record_updated_date, OLD.record_created_date,
                UUID(), NOW(), 'D'
            );
            """
        )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()

    if bind.dialect.name == "postgresql":
        op.execute("DROP TRIGGER IF EXISTS record_resource_history on resources;")
        op.execute(
            "DROP TRIGGER IF EXISTS record_resource_attributes_history on resource_attributes;"
        )
        op.execute("DROP TRIGGER IF EXISTS record_principal_history on principals;")
        op.execute(
            "DROP TRIGGER IF EXISTS record_principal_attributes_history on principal_attributes;"
        )
        op.execute("DROP FUNCTION IF EXISTS record_history();")

    elif bind.dialect.name == "mysql":
        # Drop all MySQL triggers
        op.execute("DROP TRIGGER IF EXISTS record_resource_history_insert;")
        op.execute("DROP TRIGGER IF EXISTS record_resource_history_update;")
        op.execute("DROP TRIGGER IF EXISTS record_resource_history_delete;")

        op.execute("DROP TRIGGER IF EXISTS record_resource_attributes_history_insert;")
        op.execute("DROP TRIGGER IF EXISTS record_resource_attributes_history_update;")
        op.execute("DROP TRIGGER IF EXISTS record_resource_attributes_history_delete;")

        op.execute("DROP TRIGGER IF EXISTS record_principal_history_insert;")
        op.execute("DROP TRIGGER IF EXISTS record_principal_history_update;")
        op.execute("DROP TRIGGER IF EXISTS record_principal_history_delete;")

        op.execute("DROP TRIGGER IF EXISTS record_principal_attributes_history_insert;")
        op.execute("DROP TRIGGER IF EXISTS record_principal_attributes_history_update;")
        op.execute("DROP TRIGGER IF EXISTS record_principal_attributes_history_delete;")
