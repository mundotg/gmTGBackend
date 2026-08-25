"""expand security_test_attempts with full request/response capture

Guarda, por tentativa: método, endpoint, headers e corpo do pedido, headers e
corpo da resposta, e o tamanho. Antes só se guardava status/latência/outcome.

Revision ID: f5a8c3d71b64
Revises: e4f7b2c60a93
Create Date: 2026-08-08

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f5a8c3d71b64"
down_revision: Union[str, None] = "e4f7b2c60a93"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("security_test_attempts", sa.Column("method", sa.String(length=10), nullable=True))
    op.add_column("security_test_attempts", sa.Column("endpoint", sa.String(length=1000), nullable=True))
    op.add_column("security_test_attempts", sa.Column("request_headers", sa.JSON(), nullable=True))
    op.add_column("security_test_attempts", sa.Column("request_body", sa.Text(), nullable=True))
    op.add_column("security_test_attempts", sa.Column("response_headers", sa.JSON(), nullable=True))
    op.add_column("security_test_attempts", sa.Column("response_body", sa.Text(), nullable=True))
    op.add_column(
        "security_test_attempts",
        sa.Column("response_size", sa.Integer(), nullable=True, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("security_test_attempts", "response_size")
    op.drop_column("security_test_attempts", "response_body")
    op.drop_column("security_test_attempts", "response_headers")
    op.drop_column("security_test_attempts", "request_body")
    op.drop_column("security_test_attempts", "request_headers")
    op.drop_column("security_test_attempts", "endpoint")
    op.drop_column("security_test_attempts", "method")
