"""cria tabela email_verification_tokens

Tabela para registrar os tokens de confirmação de e-mail dos utilizadores.
Relaciona-se com a tabela users e armazena:
- token: o hash/código de verificação
- user_id: FK para users.id
- created_at: data em que o token foi criado
- expires_at: prazo limite de validade do token
- validated_at: data em que a verificação foi confirmada
- is_used: indicador se o token já foi consumido

Revision ID: f6b8c9d01e23
Revises: e1a7c3d95f26
Create Date: 2026-09-29

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f6b8c9d01e23"
down_revision: Union[str, None] = "e1a7c3d95f26"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "email_verification_tokens",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("token", sa.String(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_used", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )
    op.create_index(
        "ix_email_verification_tokens_token",
        "email_verification_tokens",
        ["token"],
        unique=True,
    )
    op.create_index(
        "ix_email_verification_tokens_user_id",
        "email_verification_tokens",
        ["user_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_email_verification_tokens_user_id",
        table_name="email_verification_tokens",
    )
    op.drop_index(
        "ix_email_verification_tokens_token",
        table_name="email_verification_tokens",
    )
    op.drop_table("email_verification_tokens")
