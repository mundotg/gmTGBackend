"""cria tabela relacao n por n empresa_connections

Tabela de associação N:N entre empresas e db_connections:
- empresa_id (FK empresas.id, ON DELETE CASCADE, ON UPDATE CASCADE)
- connection_id (FK db_connections.id, ON DELETE CASCADE, ON UPDATE CASCADE)
- created_at (DateTime)

Revision ID: c4d5e6f7a8b9
Revises: b2c3d4e5f6a7
Create Date: 2026-10-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c4d5e6f7a8b9"
down_revision: Union[str, None] = "f6a7b8c9d0e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    existing_tables = set(insp.get_table_names())

    if "empresa_connections" not in existing_tables:
        op.create_table(
            "empresa_connections",
            sa.Column(
                "empresa_id",
                sa.Integer(),
                sa.ForeignKey("empresas.id", ondelete="CASCADE", onupdate="CASCADE"),
                primary_key=True,
                nullable=False,
            ),
            sa.Column(
                "connection_id",
                sa.Integer(),
                sa.ForeignKey("db_connections.id", ondelete="CASCADE", onupdate="CASCADE"),
                primary_key=True,
                nullable=False,
            ),
            sa.Column(
                "created_at",
                sa.DateTime(),
                server_default=sa.func.now(),
                nullable=True,
            ),
        )
        op.create_index(
            "ix_empresa_connections_empresa_id",
            "empresa_connections",
            ["empresa_id"],
        )
        op.create_index(
            "ix_empresa_connections_connection_id",
            "empresa_connections",
            ["connection_id"],
        )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    existing_tables = set(insp.get_table_names())

    if "empresa_connections" in existing_tables:
        op.drop_index(
            "ix_empresa_connections_connection_id",
            table_name="empresa_connections",
        )
        op.drop_index(
            "ix_empresa_connections_empresa_id",
            table_name="empresa_connections",
        )
        op.drop_table("empresa_connections")
