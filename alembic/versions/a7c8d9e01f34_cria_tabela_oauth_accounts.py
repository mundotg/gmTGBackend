"""cria tabela oauth_accounts

Tabela para armazenar as contas e dados retornados pelos provedores de autenticação (OAuth2 / Auth0):
- Google
- GitHub
- GitLab
- Microsoft (Azure AD / Entra ID)
- Auth0 (ou outros provedores)

Relaciona-se com a tabela users e armazena:
- id: identificador interno único
- user_id: FK para users.id
- provider: nome do provedor (google, github, gitlab, microsoft, etc.)
- provider_user_id: ID do utilizador no serviço externo
- email: e-mail fornecido pelo provedor externo
- name: nome de exibição do utilizador
- avatar_url: URL da foto/avatar
- access_token: token de acesso obtido do provedor (opcional)
- refresh_token: token de renovação (opcional)
- expires_at: expiração do token do provedor
- raw_data: payload JSON bruto retornado pela API do provedor
- created_at: data do primeiro vínculo
- updated_at: data da última atualização/login

Revision ID: a7c8d9e01f34
Revises: f6b8c9d01e23
Create Date: 2026-10-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a7c8d9e01f34"
down_revision: Union[str, None] = "f6b8c9d01e23"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Cria a tabela oauth_accounts e seus respectivos índices e constraints."""
    op.create_table(
        "oauth_accounts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=50), nullable=False),
        sa.Column("provider_user_id", sa.String(length=255), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("avatar_url", sa.String(), nullable=True),
        sa.Column("access_token", sa.String(), nullable=True),
        sa.Column("refresh_token", sa.String(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("raw_data", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        sa.UniqueConstraint(
            "provider",
            "provider_user_id",
            name="uq_oauth_provider_user_id",
        ),
    )
    op.create_index(
        "ix_oauth_accounts_id",
        "oauth_accounts",
        ["id"],
        unique=False,
    )
    op.create_index(
        "ix_oauth_accounts_user_id",
        "oauth_accounts",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_oauth_accounts_provider",
        "oauth_accounts",
        ["provider"],
        unique=False,
    )
    op.create_index(
        "ix_oauth_accounts_provider_user_id",
        "oauth_accounts",
        ["provider_user_id"],
        unique=False,
    )
    op.create_index(
        "ix_oauth_accounts_email",
        "oauth_accounts",
        ["email"],
        unique=False,
    )


def downgrade() -> None:
    """Remove a tabela oauth_accounts e índices."""
    op.drop_index("ix_oauth_accounts_email", table_name="oauth_accounts")
    op.drop_index("ix_oauth_accounts_provider_user_id", table_name="oauth_accounts")
    op.drop_index("ix_oauth_accounts_provider", table_name="oauth_accounts")
    op.drop_index("ix_oauth_accounts_user_id", table_name="oauth_accounts")
    op.drop_index("ix_oauth_accounts_id", table_name="oauth_accounts")
    op.drop_table("oauth_accounts")
