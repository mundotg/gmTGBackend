from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey
)
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from app.database import Base


class Settings(Base):
    __tablename__ = "settings"

    id = Column(Integer, primary_key=True, index=True)

    # 🔗 Relação 1:1 com User
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True
    )

    # ⚙️ Preferências do utilizador
    theme = Column(String(20), default="light", nullable=False)
    language = Column(String(10), default="pt", nullable=False)
    sidebar_collapsed = Column(Boolean, default=False, nullable=False)
    preferred_db_type = Column(String(50))

    # 🕒 Auditoria
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False
    )

    # 🔁 ORM relationship
    user = relationship(
        "User",
        back_populates="settings",
        foreign_keys=[user_id],
        uselist=False
    )

    def __repr__(self):
        return (
            f"<Settings(id={self.id}, user_id={self.user_id}, "
            f"theme='{self.theme}', language='{self.language}')>"
        )
