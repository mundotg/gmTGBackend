"""roles: nome único por empresa (e entre as globais)

O UNIQUE(name) original impedia duas empresas de terem um cargo com o mesmo
nome (ex.: "auditor"). A b2c3d4e5f6a7 acrescentou roles.empresa_id mas não
trocou essa restrição.

Fica:
  * uq_roles_name_global  → nome único entre as funções globais (empresa_id NULL)
  * uq_roles_name_empresa → nome único dentro de cada empresa

São índices parciais porque em PostgreSQL um UNIQUE(name, empresa_id) simples
não trava duas globais com o mesmo nome (NULL nunca colide com NULL).

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-10-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c3d4e5f6a7b8"
down_revision: Union[str, None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_GLOBAL = sa.text("empresa_id IS NULL")
_EMPRESA = sa.text("empresa_id IS NOT NULL")


def upgrade() -> None:
    conn = op.get_bind()
    if "roles" not in sa.inspect(conn).get_table_names():
        return

    # 1. Restrições/índices únicos só sobre `name` (o roles_name_key original)
    #    e o UNIQUE(name, empresa_id) não parcial, se alguém o tiver criado.
    a_trocar = (["name"], ["name", "empresa_id"])
    for uc in sa.inspect(conn).get_unique_constraints("roles"):
        if uc["column_names"] in a_trocar:
            op.drop_constraint(uc["name"], "roles", type_="unique")
    # Novo inspector: largar a restrição já largou o índice que a suporta.
    for ix in sa.inspect(conn).get_indexes("roles"):
        if ix.get("unique") and ix["column_names"] in a_trocar and not ix.get("dialect_options"):
            op.drop_index(ix["name"], table_name="roles")

    existentes = {ix["name"] for ix in sa.inspect(conn).get_indexes("roles")}

    if "uq_roles_name_global" not in existentes:
        op.create_index(
            "uq_roles_name_global", "roles", ["name"], unique=True,
            postgresql_where=_GLOBAL, sqlite_where=_GLOBAL,
        )
    if "uq_roles_name_empresa" not in existentes:
        op.create_index(
            "uq_roles_name_empresa", "roles", ["name", "empresa_id"], unique=True,
            postgresql_where=_EMPRESA, sqlite_where=_EMPRESA,
        )
    # A b2c3d4e5f6a7 só cria este índice quando ela própria adiciona a coluna.
    if "ix_roles_empresa_id" not in existentes:
        op.create_index("ix_roles_empresa_id", "roles", ["empresa_id"])


def downgrade() -> None:
    conn = op.get_bind()
    if "roles" not in sa.inspect(conn).get_table_names():
        return
    existentes = {ix["name"] for ix in sa.inspect(conn).get_indexes("roles")}
    for nome in ("uq_roles_name_empresa", "uq_roles_name_global"):
        if nome in existentes:
            op.drop_index(nome, table_name="roles")
    # Falha se entretanto existirem nomes repetidos entre empresas — de propósito:
    # repor o UNIQUE(name) nesse estado perderia a distinção sem avisar.
    op.create_unique_constraint("roles_name_key", "roles", ["name"])
