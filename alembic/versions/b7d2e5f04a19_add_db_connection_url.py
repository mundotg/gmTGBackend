"""add url to db_connections

Modo "ligar por URL": a connection string completa do fornecedor (Neon,
Supabase, Atlas, Railway…) é guardada cifrada em repouso e usada tal como está,
em vez de ser remontada a partir dos campos separados.

Nullable de propósito: as conexões existentes continuam a usar os campos
separados e não precisam de qualquer conversão.

Revision ID: b7d2e5f04a19
Revises: a6b9d4e28c71
Create Date: 2026-08-17

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b7d2e5f04a19"
down_revision: Union[str, None] = "a6b9d4e28c71"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("db_connections", sa.Column("url", sa.String(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("db_connections", "url")
