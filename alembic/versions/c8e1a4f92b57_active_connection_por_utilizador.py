"""active_connection passa a ser por utilizador

A conexão ativa era guardada com `connection_id` como chave única, ou seja,
"estar ligada" era uma propriedade da conexão e não de quem a usa. Numa conexão
partilhada por duas pessoas isso significava uma linha só para ambas: ligar de
um lado desligava o outro, e `get_connection_current` filtrava por
`DBConnections.user_id`, pelo que só o dono conseguia sequer trabalhar.

Com `user_id` na chave, cada utilizador tem o seu próprio estado de ligação
sobre a mesma conexão. Quem pode ligar-se a quê continua a ser decidido à parte,
por `app.ultils.connection_access`.

A tabela é recriada em vez de alterada porque mudar a chave primária não é
portável entre SQLite e PostgreSQL. As linhas existentes são atribuídas ao DONO
da conexão, que reproduz exatamente o comportamento anterior.

Revision ID: c8e1a4f92b57
Revises: b7d2e5f04a19
Create Date: 2026-08-29

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c8e1a4f92b57"
down_revision: Union[str, None] = "b7d2e5f04a19"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("CREATE TABLE active_connection_bkp AS SELECT * FROM active_connection")
    op.drop_table("active_connection")

    op.create_table(
        "active_connection",
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "connection_id",
            sa.Integer(),
            sa.ForeignKey("db_connections.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("activated_at", sa.DateTime(), nullable=False),
        sa.Column("status", sa.Boolean(), nullable=False),
        sa.Column("last_checked", sa.DateTime(), nullable=True),
    )
    op.create_index(
        "ix_active_connection_user_id", "active_connection", ["user_id"]
    )

    # Cada ligação existente passa a pertencer ao dono da conexão. O JOIN também
    # descarta linhas órfãs (conexão já apagada), que não teriam dono a quem ir.
    op.execute(
        """
        INSERT INTO active_connection
            (user_id, connection_id, activated_at, status, last_checked)
        SELECT c.user_id, b.connection_id, b.activated_at, b.status, b.last_checked
        FROM active_connection_bkp b
        JOIN db_connections c ON c.id = b.connection_id
        """
    )

    op.drop_table("active_connection_bkp")


def downgrade() -> None:
    """
    Downgrade schema.

    Volta a uma linha por conexão. As ligações de quem tinha a conexão apenas
    por partilha não têm lugar no modelo antigo e são descartadas — só sobrevive
    a linha do dono.
    """
    op.execute("CREATE TABLE active_connection_bkp AS SELECT * FROM active_connection")
    op.drop_table("active_connection")

    op.create_table(
        "active_connection",
        sa.Column(
            "connection_id",
            sa.Integer(),
            sa.ForeignKey("db_connections.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("activated_at", sa.DateTime(), nullable=False),
        sa.Column("status", sa.Boolean(), nullable=False),
        sa.Column("last_checked", sa.DateTime(), nullable=True),
    )

    op.execute(
        """
        INSERT INTO active_connection
            (connection_id, activated_at, status, last_checked)
        SELECT b.connection_id, b.activated_at, b.status, b.last_checked
        FROM active_connection_bkp b
        JOIN db_connections c
          ON c.id = b.connection_id AND c.user_id = b.user_id
        """
    )

    op.drop_table("active_connection_bkp")
