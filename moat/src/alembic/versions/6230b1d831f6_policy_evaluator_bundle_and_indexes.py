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
TRIGRAM_COLUMNS = ["principal_fq_name", "resource_fq_name"]


def upgrade() -> None:
    """Upgrade schema."""
    # copied from opa_bundles rather than referenced, as old bundles are pruned
    op.add_column(TABLE_NAME, sa.Column("bundle_e_tag", sa.String(length=255), nullable=True))
    op.add_column(TABLE_NAME, sa.Column("bundle_policy_hash", sa.String(length=255), nullable=True))

    # ACL API default sort and newer_than filter; id matches its tie-break ordering
    op.create_index(
        f"ix_{TABLE_NAME}_record_created_date",
        TABLE_NAME,
        ["record_created_date", "id"],
    )
    # ACL API resource_prefix filter (LIKE 'prefix%'). Postgres needs pattern ops
    # for LIKE under a non-C collation; MySQL caps index keys at 3072 bytes
    op.create_index(
        f"ix_{TABLE_NAME}_resource_fq_name",
        TABLE_NAME,
        ["resource_fq_name"],
        postgresql_ops={"resource_fq_name": "varchar_pattern_ops"},
        mysql_length={"resource_fq_name": 255},
    )

    # ACL API search (ILIKE '%term%') can only use trigram indexes, which are Postgres-only
    if op.get_bind().dialect.name != "postgresql":
        return

    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    for column in TRIGRAM_COLUMNS:
        op.create_index(
            f"ix_{TABLE_NAME}_{column}_trgm",
            TABLE_NAME,
            [column],
            postgresql_using="gin",
            postgresql_ops={column: "gin_trgm_ops"},
        )


def downgrade() -> None:
    """Downgrade schema."""
    if op.get_bind().dialect.name == "postgresql":
        for column in TRIGRAM_COLUMNS:
            op.drop_index(f"ix_{TABLE_NAME}_{column}_trgm", table_name=TABLE_NAME)
        # pg_trgm is left installed as other objects may depend on it

    op.drop_index(f"ix_{TABLE_NAME}_resource_fq_name", table_name=TABLE_NAME)
    op.drop_index(f"ix_{TABLE_NAME}_record_created_date", table_name=TABLE_NAME)
    op.drop_column(TABLE_NAME, "bundle_policy_hash")
    op.drop_column(TABLE_NAME, "bundle_e_tag")
