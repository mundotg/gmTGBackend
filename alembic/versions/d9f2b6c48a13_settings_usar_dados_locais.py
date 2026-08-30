"""settings: usar_dados_locais

Preferencia por utilizador para ignorar metadados em cache (nomes de tabelas,
colunas, enums, contagens) e ler sempre da origem.

Default True: mantem o comportamento atual para toda a gente. Quem quiser ver o
estado real do schema desliga-a, e passa a pagar a consulta em cada pedido.

Revision ID: d9f2b6c48a13
Revises: 42e3afe88eff
Create Date: 2026-08-30

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d9f2b6c48a13"
down_revision: Union[str, None] = "42e3afe88eff"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Entra nullable para as linhas existentes nao violarem o NOT NULL,
    # preenche-se, e so depois se aperta.
    op.add_column(
        "settings",
        sa.Column("usar_dados_locais", sa.Boolean(), nullable=True),
    )
    op.execute("UPDATE settings SET usar_dados_locais = true WHERE usar_dados_locais IS NULL")
    op.alter_column(
        "settings",
        "usar_dados_locais",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.true(),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("settings", "usar_dados_locais")
