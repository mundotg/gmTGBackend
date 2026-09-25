"""align task/sprint columns with schemas

Os schemas (TaskSchema, SprintSchema) e o frontend já usavam estes campos, mas
as colunas nunca foram criadas: criar tarefa/sprint rebentava e ativar/cancelar
sprint era silenciosamente ignorado.

Revision ID: c2d5f8b31e47
Revises: b1c4e7a90d31
Create Date: 2026-08-08

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c2d5f8b31e47"
down_revision: Union[str, None] = "b1c4e7a90d31"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # --- tasks ---------------------------------------------------------
    op.add_column(
        "tasks",
        sa.Column("estimated_hours", sa.Float(), nullable=True, server_default="0"),
    )
    op.add_column("tasks", sa.Column("tags", sa.JSON(), nullable=True))
    op.add_column("tasks", sa.Column("schedule", sa.JSON(), nullable=True))
    op.add_column("tasks", sa.Column("is_validated", sa.Boolean(), nullable=True))
    op.add_column(
        "tasks", sa.Column("comentario_is_validated", sa.String(length=500), nullable=True)
    )
    op.add_column("tasks", sa.Column("validated_by_id", sa.Integer(), nullable=True))
    op.add_column("tasks", sa.Column("validated_at", sa.DateTime(), nullable=True))
    op.create_foreign_key(
        "fk_tasks_validated_by_id_users",
        "tasks",
        "users",
        ["validated_by_id"],
        ["id"],
        ondelete="SET NULL",
    )

    # --- sprints -------------------------------------------------------
    op.add_column("sprints", sa.Column("goal", sa.String(length=255), nullable=True))
    op.add_column(
        "sprints",
        sa.Column(
            "is_active", sa.Boolean(), nullable=False, server_default=sa.true()
        ),
    )
    op.add_column(
        "sprints",
        sa.Column(
            "cancelled", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
    )
    op.add_column(
        "sprints", sa.Column("motivo_cancelamento", sa.String(length=255), nullable=True)
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("sprints", "motivo_cancelamento")
    op.drop_column("sprints", "cancelled")
    op.drop_column("sprints", "is_active")
    op.drop_column("sprints", "goal")

    op.drop_constraint("fk_tasks_validated_by_id_users", "tasks", type_="foreignkey")
    op.drop_column("tasks", "validated_at")
    op.drop_column("tasks", "validated_by_id")
    op.drop_column("tasks", "comentario_is_validated")
    op.drop_column("tasks", "is_validated")
    op.drop_column("tasks", "schedule")
    op.drop_column("tasks", "tags")
    op.drop_column("tasks", "estimated_hours")
