"""adiciona regras avancadas e acesso empresa nas conexoes

Campos adicionados:
- empresa_connections: access_level, role_id
- connection_roles: allowed_tables, blocked_tables, allowed_columns, blocked_columns, allowed_query_types, max_rows
- db_connection_shares: allowed_tables, blocked_tables, allowed_columns, blocked_columns, allowed_query_types, max_rows

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-10-02

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d5e6f7a8b9c0"
down_revision: Union[str, None] = "c4d5e6f7a8b9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    existing_tables = set(insp.get_table_names())

    # 1. empresa_connections (access_level e role_id)
    if "empresa_connections" in existing_tables:
        cols = {c["name"] for c in insp.get_columns("empresa_connections")}
        if "access_level" not in cols:
            op.add_column(
                "empresa_connections",
                sa.Column("access_level", sa.String(length=20), server_default="read", nullable=False),
            )
        if "role_id" not in cols:
            op.add_column(
                "empresa_connections",
                sa.Column("role_id", sa.Integer(), sa.ForeignKey("connection_roles.id", ondelete="SET NULL"), nullable=True),
            )
            op.create_index("ix_empresa_connections_role_id", "empresa_connections", ["role_id"])

    # 2. connection_roles (regras avançadas)
    if "connection_roles" in existing_tables:
        cols = {c["name"] for c in insp.get_columns("connection_roles")}
        for col_name in ("allowed_tables", "blocked_tables", "allowed_columns", "blocked_columns", "allowed_query_types"):
            if col_name not in cols:
                op.add_column("connection_roles", sa.Column(col_name, sa.JSON(), nullable=True))
        if "max_rows" not in cols:
            op.add_column("connection_roles", sa.Column("max_rows", sa.Integer(), nullable=True))

    # 3. db_connection_shares (regras avançadas customizadas)
    if "db_connection_shares" in existing_tables:
        cols = {c["name"] for c in insp.get_columns("db_connection_shares")}
        for col_name in ("allowed_tables", "blocked_tables", "allowed_columns", "blocked_columns", "allowed_query_types"):
            if col_name not in cols:
                op.add_column("db_connection_shares", sa.Column(col_name, sa.JSON(), nullable=True))
        if "max_rows" not in cols:
            op.add_column("db_connection_shares", sa.Column("max_rows", sa.Integer(), nullable=True))


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    existing_tables = set(insp.get_table_names())

    if "db_connection_shares" in existing_tables:
        cols = {c["name"] for c in insp.get_columns("db_connection_shares")}
        for col_name in ("allowed_tables", "blocked_tables", "allowed_columns", "blocked_columns", "allowed_query_types", "max_rows"):
            if col_name in cols:
                op.drop_column("db_connection_shares", col_name)

    if "connection_roles" in existing_tables:
        cols = {c["name"] for c in insp.get_columns("connection_roles")}
        for col_name in ("allowed_tables", "blocked_tables", "allowed_columns", "blocked_columns", "allowed_query_types", "max_rows"):
            if col_name in cols:
                op.drop_column("connection_roles", col_name)

    if "empresa_connections" in existing_tables:
        cols = {c["name"] for c in insp.get_columns("empresa_connections")}
        if "role_id" in cols:
            op.drop_index("ix_empresa_connections_role_id", table_name="empresa_connections")
            op.drop_column("empresa_connections", "role_id")
        if "access_level" in cols:
            op.drop_column("empresa_connections", "access_level")
