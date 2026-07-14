"""add_policy_evaluator_results_table

Revision ID: f1a2b3c4d5e6
Revises: e1833e19df8b
Create Date: 2026-04-18 00:00:00.000000+00:00

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "f1a2b3c4d5e6"
down_revision: Union[str, Sequence[str], None] = "e1833e19df8b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "policy_evaluator_results",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("principal", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.Text(), nullable=False),
        sa.Column("resource", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_policy_evaluator_results_principal",
        "policy_evaluator_results",
        ["principal"],
    )
    op.create_index(
        "ix_policy_evaluator_results_resource",
        "policy_evaluator_results",
        ["resource"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_policy_evaluator_results_resource", table_name="policy_evaluator_results")
    op.drop_index("ix_policy_evaluator_results_principal", table_name="policy_evaluator_results")
    op.drop_table("policy_evaluator_results")
