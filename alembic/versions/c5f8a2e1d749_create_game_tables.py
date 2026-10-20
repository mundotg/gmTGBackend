"""create_game_tables

Revision ID: c5f8a2e1d749
Revises: b3e8a1d49c25
Create Date: 2026-10-19 12:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c5f8a2e1d749'
down_revision: Union[str, None] = 'b3e8a1d49c25'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing_tables = set(inspector.get_table_names())

    # 1. Tabela game_sessions
    if 'game_sessions' not in existing_tables:
        op.create_table(
            'game_sessions',
            sa.Column('id', sa.Uuid(), nullable=False),
            sa.Column('room_code', sa.String(length=20), nullable=True),
            sa.Column('mode', sa.String(length=20), nullable=False, server_default='local'),
            sa.Column('status', sa.String(length=20), nullable=False, server_default='active'),
            sa.Column('player1_id', sa.Integer(), nullable=True),
            sa.Column('player1_name', sa.String(length=100), nullable=False, server_default='Jogador 1'),
            sa.Column('player2_id', sa.Integer(), nullable=True),
            sa.Column('player2_name', sa.String(length=100), nullable=False, server_default='Jogador 2'),
            sa.Column('current_turn', sa.Integer(), nullable=False, server_default='1'),
            sa.Column('winner_id', sa.Integer(), nullable=True),
            sa.Column('winner_player', sa.Integer(), nullable=True),
            sa.Column('winning_line', sa.String(length=50), nullable=True),
            sa.Column('difficulty', sa.String(length=20), nullable=True, server_default='medium'),
            sa.Column('board_state', sa.JSON(), nullable=True),
            sa.Column('total_moves', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('duration_seconds', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column('finished_at', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['player1_id'], ['users.id'], ondelete='SET NULL'),
            sa.ForeignKeyConstraint(['player2_id'], ['users.id'], ondelete='SET NULL'),
            sa.ForeignKeyConstraint(['winner_id'], ['users.id'], ondelete='SET NULL'),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index('ix_game_sessions_room_code', 'game_sessions', ['room_code'], unique=True)
        op.create_index('ix_game_sessions_mode', 'game_sessions', ['mode'])
        op.create_index('ix_game_sessions_status', 'game_sessions', ['status'])
        op.create_index('ix_game_sessions_player1_id', 'game_sessions', ['player1_id'])
        op.create_index('ix_game_sessions_player2_id', 'game_sessions', ['player2_id'])

    # 2. Tabela game_moves
    if 'game_moves' not in existing_tables:
        op.create_table(
            'game_moves',
            sa.Column('id', sa.Uuid(), nullable=False),
            sa.Column('game_id', sa.Uuid(), nullable=False),
            sa.Column('step', sa.Integer(), nullable=False),
            sa.Column('player', sa.Integer(), nullable=False),
            sa.Column('from_casa', sa.Integer(), nullable=False),
            sa.Column('to_casa', sa.Integer(), nullable=False),
            sa.Column('board_before', sa.JSON(), nullable=True),
            sa.Column('board_after', sa.JSON(), nullable=True),
            sa.Column('tactical_intent', sa.String(length=255), nullable=True),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.ForeignKeyConstraint(['game_id'], ['game_sessions.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index('ix_game_moves_game_id', 'game_moves', ['game_id'])
        op.create_index('ix_game_moves_step', 'game_moves', ['step'])

    # 3. Tabela game_chats
    if 'game_chats' not in existing_tables:
        op.create_table(
            'game_chats',
            sa.Column('id', sa.Uuid(), nullable=False),
            sa.Column('game_id', sa.Uuid(), nullable=False),
            sa.Column('sender_player', sa.Integer(), nullable=False, server_default='1'),
            sa.Column('sender_name', sa.String(length=100), nullable=False),
            sa.Column('user_id', sa.Integer(), nullable=True),
            sa.Column('message', sa.Text(), nullable=False),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.ForeignKeyConstraint(['game_id'], ['game_sessions.id'], ondelete='CASCADE'),
            sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='SET NULL'),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index('ix_game_chats_game_id', 'game_chats', ['game_id'])

    # 4. Tabela game_player_stats
    if 'game_player_stats' not in existing_tables:
        op.create_table(
            'game_player_stats',
            sa.Column('id', sa.Uuid(), nullable=False),
            sa.Column('user_id', sa.Integer(), nullable=True),
            sa.Column('player_name', sa.String(length=100), nullable=False),
            sa.Column('total_games', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('wins_local', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('wins_online', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('wins_vs_ai', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('losses', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('draws', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('total_moves', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('center_captures', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('win_streak', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('best_win_streak', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
        )
        op.create_index('ix_game_player_stats_user_id', 'game_player_stats', ['user_id'], unique=True)
        op.create_index('ix_game_player_stats_player_name', 'game_player_stats', ['player_name'])


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing_tables = set(inspector.get_table_names())

    if 'game_player_stats' in existing_tables:
        op.drop_table('game_player_stats')
    if 'game_chats' in existing_tables:
        op.drop_table('game_chats')
    if 'game_moves' in existing_tables:
        op.drop_table('game_moves')
    if 'game_sessions' in existing_tables:
        op.drop_table('game_sessions')
