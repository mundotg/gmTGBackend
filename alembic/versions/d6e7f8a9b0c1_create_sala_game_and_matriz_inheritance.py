"""create_sala_game_and_matriz_inheritance

Revision ID: d6e7f8a9b0c1
Revises: c5f8a2e1d749
Create Date: 2026-10-19 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd6e7f8a9b0c1'
down_revision: Union[str, None] = 'c5f8a2e1d749'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing_tables = set(inspector.get_table_names())

    # 1. Tabela sala_game
    if 'sala_game' not in existing_tables:
        op.create_table(
            'sala_game',
            sa.Column('id', sa.Uuid(), nullable=False),
            sa.Column('room_code', sa.String(length=20), nullable=False),
            sa.Column('nome_sala', sa.String(length=100), nullable=False, server_default='Sala Tapatan'),
            sa.Column('host_user_id', sa.Integer(), nullable=True),
            sa.Column('guest_user_id', sa.Integer(), nullable=True),
            sa.Column('status', sa.String(length=20), nullable=False, server_default='waiting'),
            sa.Column('is_private', sa.Boolean(), nullable=False, server_default=sa.text('false')),
            sa.Column('password_hash', sa.String(length=255), nullable=True),
            sa.Column('max_players', sa.Integer(), nullable=False, server_default='2'),
            sa.Column('mode', sa.String(length=20), nullable=False, server_default='online'),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.ForeignKeyConstraint(['host_user_id'], ['users.id'], ondelete='SET NULL'),
            sa.ForeignKeyConstraint(['guest_user_id'], ['users.id'], ondelete='SET NULL'),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index('ix_sala_game_room_code', 'sala_game', ['room_code'], unique=True)
        op.create_index('ix_sala_game_status', 'sala_game', ['status'])
        op.create_index('ix_sala_game_host_user_id', 'sala_game', ['host_user_id'])
        op.create_index('ix_sala_game_guest_user_id', 'sala_game', ['guest_user_id'])

    # 2. Adicionar sala_id na tabela game_sessions
    if 'game_sessions' in existing_tables:
        columns_sessions = [c['name'] for c in inspector.get_columns('game_sessions')]
        if 'sala_id' not in columns_sessions:
            op.add_column('game_sessions', sa.Column('sala_id', sa.Uuid(), nullable=True))
            op.create_foreign_key(
                'fk_game_sessions_sala_id',
                'game_sessions',
                'sala_game',
                ['sala_id'],
                ['id'],
                ondelete='SET NULL',
            )
            op.create_index('ix_game_sessions_sala_id', 'game_sessions', ['sala_id'])

    # 3. Tabela matriz (base do tabuleiro e superclasse para GameMove)
    if 'matriz' not in existing_tables:
        op.create_table(
            'matriz',
            sa.Column('id', sa.Uuid(), nullable=False),
            sa.Column('tipo', sa.String(length=50), nullable=False, server_default='matriz'),
            sa.Column('board_state', sa.JSON(), nullable=True),
            sa.Column('casa_centro', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('rows', sa.Integer(), nullable=False, server_default='3'),
            sa.Column('cols', sa.Integer(), nullable=False, server_default='3'),
            sa.Column('casas_ativas', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index('ix_matriz_tipo', 'matriz', ['tipo'])

    # 4. Migração de dados de game_moves existentes para matriz
    if 'game_moves' in existing_tables:
        op.execute("""
            INSERT INTO matriz (id, tipo, board_state, casa_centro, rows, cols, casas_ativas, created_at)
            SELECT 
                gm.id, 
                'move', 
                gm.board_after, 
                0, 
                3, 
                3, 
                0, 
                gm.created_at 
            FROM game_moves gm
            WHERE NOT EXISTS (SELECT 1 FROM matriz m WHERE m.id = gm.id)
        """)

        # 5. Configurar herança de tabela unida (Joined Table Inheritance)
        columns_moves = [c['name'] for c in inspector.get_columns('game_moves')]
        if 'created_at' in columns_moves:
            op.drop_column('game_moves', 'created_at')

        fk_names = [fk['name'] for fk in inspector.get_foreign_keys('game_moves')]
        if 'fk_game_moves_matriz_id' not in fk_names:
            op.create_foreign_key(
                'fk_game_moves_matriz_id',
                'game_moves',
                'matriz',
                ['id'],
                ['id'],
                ondelete='CASCADE',
            )


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing_tables = set(inspector.get_table_names())

    if 'game_moves' in existing_tables:
        fk_names = [fk['name'] for fk in inspector.get_foreign_keys('game_moves')]
        if 'fk_game_moves_matriz_id' in fk_names:
            op.drop_constraint('fk_game_moves_matriz_id', 'game_moves', type_='foreignkey')
        columns_moves = [c['name'] for c in inspector.get_columns('game_moves')]
        if 'created_at' not in columns_moves:
            op.add_column('game_moves', sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()))

    if 'game_sessions' in existing_tables:
        columns_sessions = [c['name'] for c in inspector.get_columns('game_sessions')]
        if 'sala_id' in columns_sessions:
            op.drop_constraint('fk_game_sessions_sala_id', 'game_sessions', type_='foreignkey')
            op.drop_index('ix_game_sessions_sala_id', 'game_sessions')
            op.drop_column('game_sessions', 'sala_id')

    if 'matriz' in existing_tables:
        op.drop_table('matriz')

    if 'sala_game' in existing_tables:
        op.drop_table('sala_game')
