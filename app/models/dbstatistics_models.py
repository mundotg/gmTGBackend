# models/dbstatistcs_models.py

from datetime import datetime, timezone
from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import relationship
from app.database import Base

class DBStatistics(Base):
    """
    Representa estatísticas agregadas sobre uma conexão de banco de dados.
    Essas informações são úteis para exibir métricas na dashboard ou monitorar uso.
    """
    __tablename__ = "db_statistics"

    id = Column(Integer, primary_key=True, index=True)

    # Conexão à qual as estatísticas pertencem
    db_connection_id = Column(
        Integer,
        ForeignKey("db_connections.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    # Quantidade total de tabelas conectadas
    tables_connected = Column(Integer, nullable=False, default=0)

    # Métricas detalhadas
    table_count = Column(Integer, nullable=False, default=0)
    view_count = Column(Integer, nullable=False, default=0)
    procedure_count = Column(Integer, nullable=False, default=0)
    function_count = Column(Integer, nullable=False, default=0)
    trigger_count = Column(Integer, nullable=False, default=0)
    index_count = Column(Integer, nullable=False, default=0)
    server_version = Column(String, nullable=True)

    # Total de consultas feitas hoje
    queries_today = Column(Integer, nullable=False, default=0)

    # Total de registros analisados até o momento
    records_analyzed = Column(BigInteger, nullable=False, default=0)

    # Última vez que esta estatística foi atualizada
    updated_at = Column(DateTime, default=datetime.now(timezone.utc), nullable=False)

    # Momento da última consulta registrada (opcional)
    last_query_at = Column(DateTime, nullable=True)

    # Relacionamento com a tabela de conexões
    connection = relationship("DBConnection", back_populates="statistics")
    

class TableRowCountCache(Base):
    __tablename__ = "table_row_count_cache"
    __table_args__ = (
        UniqueConstraint("connection_id", "table_name", name="uix_connection_table"),
    )

    id = Column(Integer, primary_key=True, index=True)
    connection_id = Column(Integer, ForeignKey("db_connections.id", ondelete="CASCADE"), nullable=False)
    table_name = Column(String, nullable=False)
    row_count = Column(Integer, nullable=False)
    last_updated = Column(DateTime, server_default=func.now(), onupdate=func.now())
    connection = relationship("DBConnection", back_populates="row_count_cache")