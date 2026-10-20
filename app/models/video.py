import uuid
from datetime import datetime
from typing import List, Optional
import sqlalchemy as sa
from sqlalchemy import BigInteger, Column, DateTime, String, Text, func
from sqlalchemy.orm import relationship

from app.database import Base


class Video(Base):
    """
    Modelo de vídeo indexado do YouTube com métricas estatísticas e metadados.
    """
    __tablename__ = "videos"

    id = Column(sa.Uuid(), primary_key=True, default=uuid.uuid4, index=True)
    youtube_id = Column(String(20), unique=True, nullable=False, index=True)
    title = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)
    thumbnail_url = Column(String(500), nullable=True)
    channel_title = Column(String(255), nullable=True)
    category_id = Column(String(10), nullable=True, index=True)
    published_at = Column(DateTime, nullable=True, index=True)
    view_count = Column(BigInteger, nullable=False, default=0, index=True)
    like_count = Column(BigInteger, nullable=False, default=0)
    comment_count = Column(BigInteger, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, server_default=func.now(), default=datetime.utcnow)
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        default=datetime.utcnow,
        onupdate=func.now(),
    )

    # Relacionamentos 1:N
    watch_histories = relationship(
        "WatchHistory",
        back_populates="video",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    recommendations = relationship(
        "Recommendation",
        back_populates="video",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def __repr__(self) -> str:
        return f"<Video(id={self.id}, youtube_id='{self.youtube_id}', title='{self.title[:30]}')>"
