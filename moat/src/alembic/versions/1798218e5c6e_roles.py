"""roles table

Revision ID: 1798218e5c6e
Revises: 037e41dc2b6c
Create Date: 2026-11-12 22:04:40.264193+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1798218e5c6e'
down_revision: Union[str, Sequence[str], None] = '037e41dc2b6c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "roles",
        sa.Column("policy_evaluator_result_id", sa.Integer(), nullable=False),
        sa.Column("role_id", sa.String(length=256), nullable=False),
        sa.ForeignKeyConstraint(
            ["policy_evaluator_result_id"],
            ["policy_evaluator_results.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("policy_evaluator_result_id"),
    )
    op.create_index(
        "ix_roles_role_id",
        "roles",
        ["role_id"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_roles_role_id",
        table_name="roles",
    )
    op.drop_table("roles")
