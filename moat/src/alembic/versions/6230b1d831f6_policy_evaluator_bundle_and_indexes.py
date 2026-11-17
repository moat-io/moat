"""policy evaluator bundle and indexes

Revision ID: 6230b1d831f6
Revises: 037e41dc2b6c
Create Date: 2026-11-17 04:06:52.225407+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6230b1d831f6'
down_revision: Union[str, Sequence[str], None] = '037e41dc2b6c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


TABLE_NAME = "policy_evaluator_results"
# MySQL caps index keys at 3072 bytes, so the 512 char fq_names are prefix indexed there
MYSQL_LENGTH = {"principal_fq_name": 255, "resource_fq_name": 255}


def upgrade() -> None:
    """Upgrade schema."""
    # copied from opa_bundles rather than referenced, as old bundles are pruned
    op.add_column(TABLE_NAME, sa.Column("bundle_e_tag", sa.String(length=255), nullable=True))
    op.add_column(TABLE_NAME, sa.Column("bundle_policy_hash", sa.String(length=255), nullable=True))

    # what can a principal access / the evaluator merge lookup
    op.create_index(
        f"ix_{TABLE_NAME}_principal",
        TABLE_NAME,
        ["platform", "action", "principal_fq_name", "resource_fq_name"],
        mysql_length=MYSQL_LENGTH,
    )
    # who can access a resource
    op.create_index(
        f"ix_{TABLE_NAME}_resource",
        TABLE_NAME,
        ["platform", "action", "resource_fq_name", "principal_fq_name"],
        mysql_length=MYSQL_LENGTH,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(f"ix_{TABLE_NAME}_resource", table_name=TABLE_NAME)
    op.drop_index(f"ix_{TABLE_NAME}_principal", table_name=TABLE_NAME)
    op.drop_column(TABLE_NAME, "bundle_policy_hash")
    op.drop_column(TABLE_NAME, "bundle_e_tag")
