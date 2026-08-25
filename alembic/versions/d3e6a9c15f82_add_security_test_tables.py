"""add security test tables (pentest)

Histórico e estatísticas dos testes de segurança (força bruta, carga,
websocket, sse).

Revision ID: d3e6a9c15f82
Revises: c2d5f8b31e47
Create Date: 2026-08-08

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d3e6a9c15f82"
down_revision: Union[str, None] = "c2d5f8b31e47"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "security_test_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("kind", sa.String(length=30), nullable=False),
        sa.Column("label", sa.String(length=150), nullable=True),
        sa.Column("target_url", sa.String(length=1000), nullable=False),
        sa.Column("method", sa.String(length=10), nullable=True),
        sa.Column("config", sa.JSON(), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("total_attempts", sa.Integer(), nullable=True),
        sa.Column("success_count", sa.Integer(), nullable=True),
        sa.Column("failure_count", sa.Integer(), nullable=True),
        sa.Column("error_count", sa.Integer(), nullable=True),
        sa.Column("rate_limited_count", sa.Integer(), nullable=True),
        sa.Column("avg_ms", sa.Float(), nullable=True),
        sa.Column("min_ms", sa.Float(), nullable=True),
        sa.Column("max_ms", sa.Float(), nullable=True),
        sa.Column("p50_ms", sa.Float(), nullable=True),
        sa.Column("p95_ms", sa.Float(), nullable=True),
        sa.Column("p99_ms", sa.Float(), nullable=True),
        sa.Column("throughput_rps", sa.Float(), nullable=True),
        sa.Column("finding", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("duration_ms", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_security_test_runs_id"), "security_test_runs", ["id"])
    op.create_index(op.f("ix_security_test_runs_user_id"), "security_test_runs", ["user_id"])
    op.create_index(op.f("ix_security_test_runs_kind"), "security_test_runs", ["kind"])
    op.create_index(op.f("ix_security_test_runs_status"), "security_test_runs", ["status"])

    op.create_table(
        "security_test_attempts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("payload", sa.String(length=500), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Float(), nullable=True),
        sa.Column("outcome", sa.String(length=20), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["run_id"], ["security_test_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_security_test_attempts_id"), "security_test_attempts", ["id"])
    op.create_index(op.f("ix_security_test_attempts_run_id"), "security_test_attempts", ["run_id"])
    op.create_index(op.f("ix_security_test_attempts_outcome"), "security_test_attempts", ["outcome"])


def downgrade() -> None:
    op.drop_index(op.f("ix_security_test_attempts_outcome"), table_name="security_test_attempts")
    op.drop_index(op.f("ix_security_test_attempts_run_id"), table_name="security_test_attempts")
    op.drop_index(op.f("ix_security_test_attempts_id"), table_name="security_test_attempts")
    op.drop_table("security_test_attempts")

    op.drop_index(op.f("ix_security_test_runs_status"), table_name="security_test_runs")
    op.drop_index(op.f("ix_security_test_runs_kind"), table_name="security_test_runs")
    op.drop_index(op.f("ix_security_test_runs_user_id"), table_name="security_test_runs")
    op.drop_index(op.f("ix_security_test_runs_id"), table_name="security_test_runs")
    op.drop_table("security_test_runs")
