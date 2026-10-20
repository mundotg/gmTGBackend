import uuid
from datetime import datetime, timedelta
import sqlalchemy as sa
from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, String, func, text
from sqlalchemy.orm import relationship

from app.database import Base


class Recommendation(Base):
    """
    Tabela de recomendações personalizadas calculadas para os utilizadores.
    """
    __tablename__ = "recommendations"

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
    score = Column(Float, nullable=False, default=0.0)
    reason = Column(String(255), nullable=True)
    generated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        default=datetime.utcnow,
    )
    expires_at = Column(
        DateTime,
        nullable=False,
        default=lambda: datetime.utcnow() + timedelta(hours=24),
    )

    # Relacionamentos N:1
    user = relationship("User")
    video = relationship("Video", back_populates="recommendations")

    __table_args__ = (
        Index("ix_recommendations_user_score", "user_id", text("score DESC")),
    )

    def __repr__(self) -> str:
        return f"<Recommendation(id={self.id}, user_id={self.user_id}, score={self.score})>"
