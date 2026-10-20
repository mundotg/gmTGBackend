import uuid
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, func, text
from sqlalchemy.orm import relationship

from app.database import Base


class WatchHistory(Base):
    """
    Registro histórico de visualização de vídeos por utilizador.
    """
    __tablename__ = "watch_history"

    id = Column(sa.Uuid(), primary_key=True, default=uuid.uuid4, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    video_id = Column(
        sa.Uuid(),
        ForeignKey("videos.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    watched_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        default=datetime.utcnow,
    )
    watch_duration = Column(Integer, nullable=False, default=0)
    liked = Column(Boolean, nullable=False, default=False)

    # Relacionamentos N:1
    user = relationship("User")
    video = relationship("Video", back_populates="watch_histories")

    __table_args__ = (
        Index("ix_watch_history_user_watched_at", "user_id", text("watched_at DESC")),
    )

    def __repr__(self) -> str:
        return f"<WatchHistory(id={self.id}, user_id={self.user_id}, video_id={self.video_id})>"
