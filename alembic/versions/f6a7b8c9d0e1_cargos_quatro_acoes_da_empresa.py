"""cargos da empresa: só as quatro ações da empresa

Um cargo (roles.empresa_id NOT NULL) passa a poder dar apenas:
  * company:update         → editar a empresa
  * company:invite         → adicionar membros
  * company:remove_member  → remover membros
  * company:assign_cargo   → mudar o cargo dos membros (nova)

Cria a permissão nova, dá descrições legíveis às quatro e retira dos cargos
existentes tudo o resto. Lista congelada (as migrações não importam código da
aplicação).

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-10-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f6a7b8c9d0e1"
down_revision: Union[str, None] = "e5f6a7b8c9d0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ACOES = {
    "company:update": "Editar os dados da empresa",
    "company:invite": "Adicionar membros à empresa",
    "company:remove_member": "Remover membros da empresa",
    "company:assign_cargo": "Mudar o cargo dos membros da empresa",
}


def upgrade() -> None:
    conn = op.get_bind()
    tabelas = set(sa.inspect(conn).get_table_names())
    if not {"roles", "permissions", "roles_permissions"} <= tabelas:
        return

    for nome, descricao in _ACOES.items():
        existe = conn.execute(sa.text("SELECT id FROM permissions WHERE name = :n"), {"n": nome}).scalar()
        if existe is None:
            conn.execute(
                sa.text("INSERT INTO permissions (name, description) VALUES (:n, :d)"),
                {"n": nome, "d": descricao},
            )
        else:
            conn.execute(
                sa.text("UPDATE permissions SET description = :d WHERE id = :id"),
                {"d": descricao, "id": existe},
            )

    conn.execute(
        sa.text(
            """
            DELETE FROM roles_permissions rp
             USING roles r, permissions p
             WHERE rp.role_id = r.id
               AND rp.permission_id = p.id
               AND r.empresa_id IS NOT NULL
               AND NOT (p.name = ANY(:permitidas))
            """
        ),
        {"permitidas": list(_ACOES)},
    )


def downgrade() -> None:
    # Limpeza de dados: as permissões retiradas aos cargos não são repostas.
    pass
