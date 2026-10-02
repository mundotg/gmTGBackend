"""add role empresa_id and connection_roles

Adiciona isolamento de roles por empresa (roles.empresa_id)
e roles por conexão de banco de dados (connection_roles e connection_roles_permissions),
além de associar db_connection_shares.role_id.

Revision ID: b2c3d4e5f6a7
Revises: a7c8d9e01f34
Create Date: 2026-10-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, None] = "a7c8d9e01f34"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    existing_tables = set(insp.get_table_names())

    # 1. Coluna empresa_id na tabela roles
    if "roles" in existing_tables:
        roles_cols = {c["name"] for c in insp.get_columns("roles")}
        if "empresa_id" not in roles_cols:
            op.add_column(
                "roles",
                sa.Column("empresa_id", sa.Integer(), sa.ForeignKey("empresas.id", ondelete="CASCADE"), nullable=True),
            )
            op.create_index("ix_roles_empresa_id", "roles", ["empresa_id"])

    # 2. Tabela connection_roles
    if "connection_roles" not in existing_tables:
        op.create_table(
            "connection_roles",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("connection_id", sa.Integer(), sa.ForeignKey("db_connections.id", ondelete="CASCADE"), nullable=False, index=True),
            sa.Column("name", sa.String(length=50), nullable=False),
            sa.Column("description", sa.String(length=200), nullable=True),
            sa.Column("is_default", sa.Boolean(), default=False, nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=True),
            sa.UniqueConstraint("connection_id", "name", name="uq_connection_role_name"),
        )

    # 3. Tabela associativa connection_roles_permissions
    if "connection_roles_permissions" not in existing_tables:
        op.create_table(
            "connection_roles_permissions",
            sa.Column("connection_role_id", sa.Integer(), sa.ForeignKey("connection_roles.id", ondelete="CASCADE"), primary_key=True),
            sa.Column("permission_id", sa.Integer(), sa.ForeignKey("permissions.id", ondelete="CASCADE"), primary_key=True),
        )

    # 4. Coluna role_id na tabela db_connection_shares
    if "db_connection_shares" in existing_tables:
        shares_cols = {c["name"] for c in insp.get_columns("db_connection_shares")}
        if "role_id" not in shares_cols:
            op.add_column(
                "db_connection_shares",
                sa.Column("role_id", sa.Integer(), sa.ForeignKey("connection_roles.id", ondelete="SET NULL"), nullable=True),
            )
            op.create_index("ix_db_connection_shares_role_id", "db_connection_shares", ["role_id"])


def downgrade() -> None:
    conn = op.get_bind()
    insp = Inspector.from_engine(conn)
    existing_tables = set(insp.get_table_names())

    if "db_connection_shares" in existing_tables:
        shares_cols = {c["name"] for c in insp.get_columns("db_connection_shares")}
        if "role_id" in shares_cols:
            op.drop_index("ix_db_connection_shares_role_id", table_name="db_connection_shares")
            op.drop_column("db_connection_shares", "role_id")

    if "connection_roles_permissions" in existing_tables:
        op.drop_table("connection_roles_permissions")

    if "connection_roles" in existing_tables:
        op.drop_table("connection_roles")

    if "roles" in existing_tables:
        roles_cols = {c["name"] for c in insp.get_columns("roles")}
        if "empresa_id" in roles_cols:
            op.drop_index("ix_roles_empresa_id", table_name="roles")
            op.drop_column("roles", "empresa_id")
