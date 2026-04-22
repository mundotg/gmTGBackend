# ai_models.py

from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
    Index,
)
from sqlalchemy.orm import relationship
from app.database import Base


# ============================================================
# 💬 CHAT SESSION
# ============================================================
class ChatSession(Base):
    __tablename__ = "chat_sessions"

    id = Column(Integer, primary_key=True, index=True)

    user_id = Column(
        Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )

    title = Column(String(255))
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # 🔗 Relationships
    user = relationship("User", back_populates="chat_sessions")

    messages = relationship(
        "Message",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="Message.created_at",
    )

    __table_args__ = (Index("idx_chat_user", "user_id"),)


# ============================================================
# 💬 MESSAGE
# ============================================================
class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)

    session_id = Column(
        Integer,
        ForeignKey("chat_sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    role = Column(String(20), nullable=False)  # user | assistant | system
    content = Column(Text, nullable=False)

    tokens = Column(Integer)
    model_used = Column(String(100))  # ex: gemini-3.1-flash-lite

    created_at = Column(DateTime, default=datetime.utcnow)

    # 🔗 Relationships
    session = relationship("ChatSession", back_populates="messages")

    __table_args__ = (Index("idx_message_session", "session_id"),)


# ============================================================
# 📚 KNOWLEDGE BASE (RAG)
# ============================================================
class KnowledgeBase(Base):
    __tablename__ = "knowledge_base"

    id = Column(Integer, primary_key=True, index=True)

    title = Column(String(255))
    content = Column(Text, nullable=False)

    category = Column(String(100), index=True)  # database, ai, system
    source = Column(String(255))  # docs, manual, etc

    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_knowledge_category", "category"),)


# ============================================================
# 🧠 TRAINING DATA
# ============================================================
class TrainingData(Base):
    __tablename__ = "training_data"

    id = Column(Integer, primary_key=True, index=True)

    question = Column(Text, nullable=False)
    answer = Column(Text, nullable=False)

    category = Column(String(100), index=True)

    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.utcnow)


# ============================================================
# ⭐ FEEDBACK
# ============================================================
class Feedback(Base):
    __tablename__ = "feedback"

    id = Column(Integer, primary_key=True, index=True)

    message_id = Column(
        Integer, ForeignKey("messages.id", ondelete="CASCADE"), nullable=False
    )

    rating = Column(Integer)  # 1–5
    comment = Column(Text)

    created_at = Column(DateTime, default=datetime.utcnow)


# ============================================================
# 📊 USAGE LOG (custos + métricas)
# ============================================================
class UsageLog(Base):
    __tablename__ = "usage_logs"

    id = Column(Integer, primary_key=True, index=True)

    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), index=True)

    model = Column(String(100))
    tokens_input = Column(Integer)
    tokens_output = Column(Integer)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_usage_user", "user_id"),)


chat_sessions = relationship(
    "ChatSession", back_populates="user", cascade="all, delete-orphan"
)
