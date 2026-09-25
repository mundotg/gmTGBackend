"""add db_connection_shares

Partilha de conexões de BD: o dono (ou um super admin) concede acesso a outros
utilizadores, com nível read / write / manage.

Revision ID: b1c4e7a90d31
Revises: 5f6381f9ac7c
Create Date: 2026-08-07

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b1c4e7a90d31"
down_revision: Union[str, None] = "5f6381f9ac7c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "db_connection_shares",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("connection_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column(
            "access_level",
            sa.String(length=20),
            nullable=False,
            server_default="read",
        ),
        sa.Column("granted_by_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(
            ["connection_id"], ["db_connections.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["granted_by_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("connection_id", "user_id", name="uq_connection_share"),
    )
    op.create_index(
        op.f("ix_db_connection_shares_id"),
        "db_connection_shares",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_db_connection_shares_connection_id"),
        "db_connection_shares",
        ["connection_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_db_connection_shares_user_id"),
        "db_connection_shares",
        ["user_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        op.f("ix_db_connection_shares_user_id"), table_name="db_connection_shares"
    )
    op.drop_index(
        op.f("ix_db_connection_shares_connection_id"),
        table_name="db_connection_shares",
    )
    op.drop_index(op.f("ix_db_connection_shares_id"), table_name="db_connection_shares")
    op.drop_table("db_connection_shares")
