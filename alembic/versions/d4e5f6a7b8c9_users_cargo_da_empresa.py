"""users.empresa_role_id: cargo da empresa, separado do tipo de utilizador

Cada membro passa a ter:
  * role_id         → tipo de utilizador (função GLOBAL do sistema)
  * empresa_role_id → cargo (função da EMPRESA do membro)

As permissões efetivas somam as duas (User.permissions).

Quem já tinha uma função de empresa em role_id passa-a para empresa_role_id
(e fica sem tipo de utilizador), para role_id só guardar funções globais.

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-10-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, None] = "c3d4e5f6a7b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "users" not in insp.get_table_names():
        return

    cols = {c["name"] for c in insp.get_columns("users")}
    if "empresa_role_id" not in cols:
        op.add_column(
            "users",
            sa.Column(
                "empresa_role_id",
                sa.Integer(),
                sa.ForeignKey("roles.id", name="fk_users_empresa_role_id", ondelete="SET NULL"),
                nullable=True,
            ),
        )
        op.create_index("ix_users_empresa_role_id", "users", ["empresa_role_id"])

    # Funções de empresa que estavam em role_id → empresa_role_id (só se forem
    # da empresa do próprio membro; as de outra empresa são simplesmente retiradas).
    op.execute(
        """
        UPDATE users u
           SET empresa_role_id = CASE WHEN r.empresa_id = u.empresa_id THEN r.id ELSE NULL END,
               role_id = NULL
          FROM roles r
         WHERE u.role_id = r.id
           AND r.empresa_id IS NOT NULL
        """
    )


def downgrade() -> None:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    if "users" not in insp.get_table_names():
        return
    cols = {c["name"] for c in insp.get_columns("users")}
    if "empresa_role_id" not in cols:
        return
    # Devolve o cargo a role_id onde não há tipo de utilizador, para não perder acesso.
    op.execute(
        "UPDATE users SET role_id = empresa_role_id WHERE role_id IS NULL AND empresa_role_id IS NOT NULL"
    )
    op.drop_index("ix_users_empresa_role_id", table_name="users")
    op.drop_constraint("fk_users_empresa_role_id", "users", type_="foreignkey")
    op.drop_column("users", "empresa_role_id")
