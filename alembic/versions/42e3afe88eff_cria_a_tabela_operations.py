"""cria a tabela operations

A `change request` do MustaInf: liga o que se vai executar ao projeto (o
pipeline) e a conexao alvo, e guarda o estado do pedido ao longo do ciclo
de vida. Sem ela, a promessa de que nada corre fora de um pipeline ativo
nao tem onde se apoiar.

`kind`, `risk_level` e `status` sao VARCHAR com CHECK constraint, e nao ENUM
nativo do PostgreSQL: assim acrescentar um valor novo nao obriga a um ALTER
TYPE, e o mesmo esquema corre em SQLite nos testes.

O `create_constraint=True` e obrigatorio: desde o SQLAlchemy 1.4 e False por
omissao, e sem ele estas colunas seriam VARCHAR sem validacao nenhuma do lado
da base de dados.

Revision ID: 42e3afe88eff
Revises: c8e1a4f92b57
Create Date: 2026-08-30 07:43:23.872009

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '42e3afe88eff'
down_revision: Union[str, None] = 'c8e1a4f92b57'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('operations',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('uuid', sa.String(length=36), nullable=False),
    sa.Column('project_id', sa.Integer(), nullable=False),
    sa.Column('connection_id', sa.Integer(), nullable=False),
    sa.Column('task_id', sa.Integer(), nullable=True),
    sa.Column('empresa_id', sa.Integer(), nullable=True),
    sa.Column('title', sa.String(length=255), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('kind', sa.Enum('dml', 'ddl', 'script', 'backup', 'restore', 'transfer', name='operationkind', native_enum=False, create_constraint=True, length=20), nullable=False),
    sa.Column('engine_dialect', sa.String(length=30), nullable=True),
    sa.Column('payload', sa.Text(), nullable=False),
    sa.Column('impact_plan', sa.JSON(), nullable=True),
    sa.Column('risk_level', sa.Enum('low', 'medium', 'high', name='operationrisk', native_enum=False, create_constraint=True, length=20), nullable=False),
    sa.Column('rollback_payload', sa.Text(), nullable=True),
    sa.Column('is_reversible', sa.Boolean(), nullable=False),
    sa.Column('status', sa.Enum('draft', 'pending_review', 'approved', 'rejected', 'executing', 'executed', 'failed', 'rolled_back', 'cancelled', 'expired', name='operationstatus', native_enum=False, create_constraint=True, length=20), nullable=False),
    sa.Column('requested_by_id', sa.Integer(), nullable=True),
    sa.Column('submitted_at', sa.DateTime(), nullable=True),
    sa.Column('reviewed_by_id', sa.Integer(), nullable=True),
    sa.Column('decided_at', sa.DateTime(), nullable=True),
    sa.Column('review_comment', sa.String(length=500), nullable=True),
    sa.Column('expires_at', sa.DateTime(), nullable=True),
    sa.Column('executed_by_id', sa.Integer(), nullable=True),
    sa.Column('started_at', sa.DateTime(), nullable=True),
    sa.Column('finished_at', sa.DateTime(), nullable=True),
    sa.Column('duration_ms', sa.Integer(), nullable=True),
    sa.Column('rows_affected', sa.Integer(), nullable=True),
    sa.Column('error', sa.Text(), nullable=True),
    sa.Column('rolled_back_at', sa.DateTime(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['connection_id'], ['db_connections.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['empresa_id'], ['empresas.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['executed_by_id'], ['users.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['requested_by_id'], ['users.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['reviewed_by_id'], ['users.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['task_id'], ['tasks.id'], ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_operations_connection_id'), 'operations', ['connection_id'], unique=False)
    op.create_index(op.f('ix_operations_empresa_id'), 'operations', ['empresa_id'], unique=False)
    op.create_index(op.f('ix_operations_id'), 'operations', ['id'], unique=False)
    op.create_index(op.f('ix_operations_project_id'), 'operations', ['project_id'], unique=False)
    op.create_index(op.f('ix_operations_requested_by_id'), 'operations', ['requested_by_id'], unique=False)
    op.create_index(op.f('ix_operations_status'), 'operations', ['status'], unique=False)
    op.create_index(op.f('ix_operations_uuid'), 'operations', ['uuid'], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_operations_uuid'), table_name='operations')
    op.drop_index(op.f('ix_operations_status'), table_name='operations')
    op.drop_index(op.f('ix_operations_requested_by_id'), table_name='operations')
    op.drop_index(op.f('ix_operations_project_id'), table_name='operations')
    op.drop_index(op.f('ix_operations_id'), table_name='operations')
    op.drop_index(op.f('ix_operations_empresa_id'), table_name='operations')
    op.drop_index(op.f('ix_operations_connection_id'), table_name='operations')
    op.drop_table('operations')
