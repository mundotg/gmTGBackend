"""add api_tester_configs

Configuração do API tester por utilizador (env + requests guardados),
substituindo o localStorage.

Revision ID: e4f7b2c60a93
Revises: d3e6a9c15f82
Create Date: 2026-08-08

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e4f7b2c60a93"
down_revision: Union[str, None] = "d3e6a9c15f82"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "api_tester_configs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("env_vars", sa.JSON(), nullable=True),
        sa.Column("saved_requests", sa.JSON(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", name="uq_api_tester_config_user"),
    )
    op.create_index(op.f("ix_api_tester_configs_id"), "api_tester_configs", ["id"])
    op.create_index(op.f("ix_api_tester_configs_user_id"), "api_tester_configs", ["user_id"])


def downgrade() -> None:
    op.drop_index(op.f("ix_api_tester_configs_user_id"), table_name="api_tester_configs")
    op.drop_index(op.f("ix_api_tester_configs_id"), table_name="api_tester_configs")
    op.drop_table("api_tester_configs")
