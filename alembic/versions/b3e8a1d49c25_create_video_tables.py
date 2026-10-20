"""create_video_tables

Revision ID: b3e8a1d49c25
Revises: f0185cdde087
Create Date: 2026-10-19 10:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3e8a1d49c25'
down_revision: Union[str, None] = 'f0185cdde087'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Tabela videos
    op.create_table(
        'videos',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('youtube_id', sa.String(length=20), nullable=False),
        sa.Column('title', sa.String(length=500), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('thumbnail_url', sa.String(length=500), nullable=True),
        sa.Column('channel_title', sa.String(length=255), nullable=True),
        sa.Column('category_id', sa.String(length=10), nullable=True),
        sa.Column('published_at', sa.DateTime(), nullable=True),
        sa.Column('view_count', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('like_count', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('comment_count', sa.BigInteger(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('youtube_id', name='uq_videos_youtube_id'),
    )
    op.create_index('ix_videos_youtube_id', 'videos', ['youtube_id'])
    op.create_index('ix_videos_category_id', 'videos', ['category_id'])
    op.create_index('ix_videos_published_at', 'videos', ['published_at'])
    op.create_index('ix_videos_view_count', 'videos', ['view_count'])

    # 2. Tabela watch_history
    op.create_table(
        'watch_history',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('video_id', sa.Uuid(), nullable=False),
        sa.Column('watched_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('watch_duration', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('liked', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['video_id'], ['videos.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_watch_history_user_id', 'watch_history', ['user_id'])
    op.create_index('ix_watch_history_video_id', 'watch_history', ['video_id'])
    op.create_index(
        'ix_watch_history_user_watched_at',
        'watch_history',
        ['user_id', sa.text('watched_at DESC')],
    )

    # 3. Tabela recommendations
    op.create_table(
        'recommendations',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('video_id', sa.Uuid(), nullable=False),
        sa.Column('score', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('reason', sa.String(length=255), nullable=True),
        sa.Column('generated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('expires_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['video_id'], ['videos.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_recommendations_user_id', 'recommendations', ['user_id'])
    op.create_index('ix_recommendations_video_id', 'recommendations', ['video_id'])
    op.create_index(
        'ix_recommendations_user_score',
        'recommendations',
        ['user_id', sa.text('score DESC')],
    )


def downgrade() -> None:
    op.drop_index('ix_recommendations_user_score', table_name='recommendations')
    op.drop_index('ix_recommendations_video_id', table_name='recommendations')
    op.drop_index('ix_recommendations_user_id', table_name='recommendations')
    op.drop_table('recommendations')

    op.drop_index('ix_watch_history_user_watched_at', table_name='watch_history')
    op.drop_index('ix_watch_history_video_id', table_name='watch_history')
    op.drop_index('ix_watch_history_user_id', table_name='watch_history')
    op.drop_table('watch_history')

    op.drop_index('ix_videos_view_count', table_name='videos')
    op.drop_index('ix_videos_published_at', table_name='videos')
    op.drop_index('ix_videos_category_id', table_name='videos')
    op.drop_index('ix_videos_youtube_id', table_name='videos')
    op.drop_table('videos')
