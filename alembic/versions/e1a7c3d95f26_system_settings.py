"""tabela system_settings

Definicoes globais da aplicacao em chave/valor: modo de manutencao, auditoria
rigorosa, debug logs. Chave/valor em vez de uma coluna por opcao para que cada
definicao nova nao custe uma migracao.

Nao semeia linhas: a ausencia de uma chave significa "valor por omissao", que
esta declarado no `system_settings_service`. Assim uma instalacao nova comporta-se
como antes desta funcionalidade existir.

Revision ID: e1a7c3d95f26
Revises: d9f2b6c48a13
Create Date: 2026-08-30

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e1a7c3d95f26"
down_revision: Union[str, None] = "d9f2b6c48a13"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "system_settings",
        sa.Column("key", sa.String(length=64), primary_key=True),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("updated_by_id", sa.Integer(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(["updated_by_id"], ["users.id"], ondelete="SET NULL"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("system_settings")
