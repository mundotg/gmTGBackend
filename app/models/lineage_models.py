"""
🏛️ Modelos de Linhagem de Dados (Data Lineage & Flow)
Mapeia a proveniência, transformações e destino dos dados entre tabelas e bancos heterogêneos.
"""

from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    JSON,
    ForeignKey,
    Index,
)
from sqlalchemy.orm import relationship

from app.database import Base


class DataLineageEvent(Base):
    __tablename__ = "data_lineage_events"

    id = Column(Integer, primary_key=True, index=True)

    # Conexão e entidade de ORIGEM
    source_connection_id = Column(
        Integer,
        ForeignKey("db_connections.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_connection_name = Column(String(255), nullable=True)
    source_db_type = Column(String(50), nullable=True)  # postgresql, mysql, mongodb, sqlite
    source_table = Column(String(255), nullable=False, index=True)
    source_query = Column(Text, nullable=True)

    # Conexão e entidade de DESTINO
    target_connection_id = Column(
        Integer,
        ForeignKey("db_connections.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    target_connection_name = Column(String(255), nullable=True)
    target_db_type = Column(String(50), nullable=True)
    target_table = Column(String(255), nullable=False, index=True)

    # Mapeamento e regra de transformação
    columns_mapping = Column(JSON, nullable=True)
    transformation_type = Column(
        String(50), default="direct_transfer", nullable=False
    )  # direct_transfer, query_filter, aggregation, view_materialization, elt_insert
    transformation_rule = Column(Text, nullable=True)

    # Métricas da transferência/execução
    rows_transferred = Column(Integer, default=0, nullable=False)
    duration_ms = Column(Integer, default=0, nullable=False)
    status = Column(String(30), default="success", nullable=False)  # success, failed, partial
    error_message = Column(Text, nullable=True)

    # Contexto organizacional e auditoria
    user_id = Column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True
    )
    project_id = Column(
        Integer, ForeignKey("projects.id", ondelete="SET NULL"), nullable=True, index=True
    )
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )

    # Relacionamentos ORM
    source_connection = relationship(
        "DBConnection", foreign_keys=[source_connection_id]
    )
    target_connection = relationship(
        "DBConnection", foreign_keys=[target_connection_id]
    )
    user = relationship("User", foreign_keys=[user_id])
    project = relationship("Project", foreign_keys=[project_id])

    __table_args__ = (
        Index("ix_lineage_source_target", "source_table", "target_table"),
        Index("ix_lineage_status", "status"),
        Index("ix_lineage_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<DataLineageEvent(id={self.id}, {self.source_table} -> {self.target_table}, "
            f"rows={self.rows_transferred}, status={self.status})>"
        )
