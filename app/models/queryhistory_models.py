from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, Index, JSON, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from app.database import Base


class QueryHistory(Base):
    __tablename__ = "query_history"
    __table_args__ = (
        Index("ix_query_history_connection_id", "db_connection_id"),
        Index("ix_query_history_query_type", "query_type"),
        Index("ix_query_history_is_favorite", "is_favorite"),
        Index("ix_query_history_tags", "tags"),
    )

    id = Column(Integer, primary_key=True, index=True)

    # 🔹 Chaves estrangeiras
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL", onupdate="CASCADE"),
        nullable=True,
        index=True
    )
    db_connection_id = Column(
        Integer,
        ForeignKey("db_connections.id", ondelete="SET NULL", onupdate="CASCADE"),
        nullable=True,
        index=True
    )

    # 🔹 Dados da query
    query = Column(Text, nullable=False)
    query_type = Column(String(20), nullable=True)
    duration_ms = Column(Integer, nullable=True)
    result_preview = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)

    # 🔹 Favoritos e tags
    is_favorite = Column(Boolean, default=False, nullable=False)
    tags = Column(String(255), nullable=True)

    # 🔹 Auditoria e origem
    app_source = Column(String(100), nullable=True)
    client_ip = Column(String(45), nullable=True)
    executed_by = Column(String(100), nullable=True)

    # 🔹 Metadados adicionais
    meta_info = Column(JSON, nullable=True)

    # 🔹 Datas
    executed_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False
    )
    modified_by = Column(String(100), nullable=True)

    # 🔹 Relacionamentos ORM
    user = relationship("User", back_populates="query_history")
    connection = relationship("DBConnection", back_populates="queries")

    def __repr__(self):
        return (
            f"<QueryHistory(id={self.id}, user_id={self.user_id}, "
            f"db_conn={self.db_connection_id}, type={self.query_type}, "
            f"executed_at={self.executed_at}, duration={self.duration_ms}ms)>"
        )
