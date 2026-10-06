"""cargos da empresa: só permissões relacionadas com a empresa

Retira das funções de empresa (roles.empresa_id NOT NULL) as permissões que não
são da empresa (registos, sistema, gestão global, acesso a dados...). A partir
daqui o CRUD recusa-as (app/ultils/company_permissions.py) e User.permissions
ignora-as; isto limpa o que já lá estava.

A lista é uma cópia congelada de COMPANY_ROLE_PERMISSIONS na data desta
migração — as migrações não importam código da aplicação, que pode mudar.

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-10-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e5f6a7b8c9d0"
down_revision: Union[str, None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_PERMITIDAS = (
    "company:read", "company:update", "company:settings", "company:members",
    "company:invite", "company:remove_member", "company:billing",
    "team:read", "team:update", "team:manage",
    "role:read", "role:create", "role:update", "role:delete", "role:manage",
    "project:view", "project:read", "project:create", "project:update",
    "project:delete", "project:manage", "project:assign_user", "project:remove_user",
    "analytics:project:view", "analytics:project:export",
    "settings:company", "settings:team", "settings:projects",
    "db_connection:read_company",
)


def upgrade() -> None:
    conn = op.get_bind()
    tabelas = set(sa.inspect(conn).get_table_names())
    if not {"roles", "permissions", "roles_permissions"} <= tabelas:
        return
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
        {"permitidas": list(_PERMITIDAS)},
    )


def downgrade() -> None:
    # Limpeza de dados: as permissões retiradas não são repostas.
    pass
