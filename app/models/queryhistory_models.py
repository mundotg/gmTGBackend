#model/queryhistory_models.py

from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from app.database import Base


class QueryHistory(Base):
    """
    Armazena o histórico de consultas SQL feitas por um usuário em uma conexão de banco de dados.
    Usado para auditoria, recomendações, análise de performance e reuso de consultas.
    """
    __tablename__ = "query_history"

    id = Column(Integer, primary_key=True, index=True)

    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    db_connection_id = Column(Integer, ForeignKey("db_connections.id"), nullable=False)

    query = Column(Text, nullable=False)  # A query SQL executada
    query_type = Column(String, nullable=True)  # SELECT, INSERT, UPDATE, DELETE, etc. (opcional)

    executed_at = Column(DateTime, default=datetime.now(timezone.utc), nullable=False)
    duration_ms = Column(Integer, nullable=True)  # Tempo em milissegundos para execução

    result_preview = Column(Text, nullable=True)  # JSON ou string das primeiras linhas do resultado
    error_message = Column(Text, nullable=True)  # Caso a execução da query tenha falhado

    is_favorite = Column(Boolean, default=False)  # O usuário marcou como favorita?
    tags = Column(String, nullable=True)  # Ex: "clientes,análise,produtos"

    updated_at = Column(DateTime, default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc), nullable=False)

    # Relacionamentos
    user = relationship("User", back_populates="query_history")
    connection = relationship("DBConnection", back_populates="queries")

