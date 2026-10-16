from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from app.database import Base


class DataScienceAnalysis(Base):
    """
    Tabela para armazenar o resultado completo e detalhado de análises de Data Science.
    Guarda todos os dados estruturais, estatísticas descritivas por coluna,
    correlações, diagnósticos de qualidade, tendências, anomalias, segmentos e insights.
    """
    __tablename__ = "data_science_analysis"
    __table_args__ = (
        Index("ix_ds_analysis_user_id", "user_id"),
        Index("ix_ds_analysis_connection_id", "db_connection_id"),
        Index("ix_ds_analysis_base_table", "base_table"),
        Index("ix_ds_analysis_created_at", "created_at"),
        Index("ix_ds_analysis_is_favorite", "is_favorite"),
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # 🔹 Chaves estrangeiras e identificadores
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    db_connection_id = Column(
        Integer,
        ForeignKey("db_connections.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    session_id = Column(String(120), nullable=True, index=True)

    # 🔹 Identificação e consulta de origem
    title = Column(String(255), nullable=True)
    base_table = Column(String(150), nullable=True, index=True)
    query_payload = Column(JSON, nullable=True)
    sql_query = Column(Text, nullable=True)

    # 🔹 Contadores e performance
    total_rows = Column(Integer, default=0, nullable=False)
    total_columns = Column(Integer, default=0, nullable=False)
    duration_ms = Column(Float, default=0.0, nullable=True)

    # 🔹 Índice e diagnóstico de qualidade de dados
    quality_score = Column(Float, nullable=True, index=True)
    quality_details = Column(JSON, nullable=True)  # constant_columns, high_null_columns, potential_keys

    # 🔹 Visão geral da estrutura
    overview = Column(JSON, nullable=True)  # n_rows, n_cols, n_duplicated, total_cells, null_pct, memory_kb

    # 🔹 Estatísticas descritivas completas por coluna
    columns_stats = Column(JSON, nullable=True)  # métricas numéricas, histogramas, percentis, categorias, datas

    # 🔹 Estudos analíticos avançados
    correlations = Column(JSON, nullable=True)   # matriz de correlações e pares
    trend = Column(JSON, nullable=True)          # tendência temporal, direção, variação %, forecast
    anomalies = Column(JSON, nullable=True)      # contagem e registos discrepantes com scores
    segments = Column(JSON, nullable=True)       # clusters (k-means) com perfis e centróides
    insights = Column(JSON, nullable=True)       # lista de conclusões e recomendações automáticas

    # 🔹 Objeto integral completo (raw payload) para recuperação instantânea
    full_result = Column(JSON, nullable=True)

    # 🔹 Organização e anotações
    is_favorite = Column(Boolean, default=False, nullable=False)
    tags = Column(String(255), nullable=True)
    notes = Column(Text, nullable=True)

    # 🔹 Datas e auditoria
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # 🔹 Relacionamentos ORM
    user = relationship("User", back_populates="datascience_analyses")
    connection = relationship("DBConnection", back_populates="datascience_analyses")

    def __repr__(self) -> str:
        return (
            f"<DataScienceAnalysis(id={self.id}, user_id={self.user_id}, "
            f"table='{self.base_table}', rows={self.total_rows}, score={self.quality_score})>"
        )
