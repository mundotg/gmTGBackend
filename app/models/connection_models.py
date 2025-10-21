from sqlalchemy import JSON, Boolean, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from app.database import Base

from sqlalchemy import (
    Column, String, Integer, ForeignKey, DateTime, Boolean
)


class DBConnection(Base):
    __tablename__ = "db_connections"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)  # PostgreSQL, MySQL, etc.
    host = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    username = Column(String, nullable=False)
    password = Column(String, nullable=False)  # armazenar criptografado!
    database_name = Column(String, nullable=False)
    sslmode = Column(String, default="disable")
    service = Column(String, nullable=True)
    trustServerCertificate = Column(String, nullable=True)
    status = Column(String, default="available")
    is_encrypted = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # 🔹 Relacionamentos ORM
    owner = relationship("User", back_populates="db_connections")
    queries = relationship("QueryHistory", back_populates="connection", cascade="all, delete-orphan", passive_deletes=True)
    statistics = relationship("DBStatistics", back_populates="connection", uselist=False, passive_deletes=True)
    logs = relationship("ConnectionLog", back_populates="connection", passive_deletes=True)
    structures = relationship("DBStructure", back_populates="connection", cascade="all, delete-orphan", lazy="selectin")
    row_count_cache = relationship("TableRowCountCache", back_populates="connection", cascade="all, delete-orphan")
    health_checks = relationship("DBHealthCheck", back_populates="connection", cascade="all, delete-orphan", passive_deletes=True)

    # 🧩 Novo relacionamento com projetos
    projects = relationship("Project", back_populates="db_connection", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<DBConnection(id={self.id}, name='{self.name}', type='{self.type}')>"


class ActiveConnection(Base):
    __tablename__ = "active_connection"

    connection_id = Column(Integer, ForeignKey("db_connections.id", ondelete="CASCADE"), primary_key=True)
    activated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    status = Column(Boolean, default=False, nullable=False)
    last_checked = Column(DateTime, nullable=True)

    connection = relationship("DBConnection", backref="active_status")


class ConnectionLog(Base):
    __tablename__ = "connection_logs"

    id = Column(Integer, primary_key=True, index=True)
    connection_id = Column(Integer, ForeignKey("db_connections.id", ondelete="CASCADE"), nullable=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    action = Column(String, nullable=True)
    details = Column(JSON, nullable=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    status = Column(String, default="success")

    connection = relationship("DBConnection", back_populates="logs")
    
class DBHealthCheck(Base):
    __tablename__ = "db_health_checks"

    id = Column(Integer, primary_key=True)
    connection_id = Column(Integer, ForeignKey("db_connections.id", ondelete="CASCADE"))
    checked_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    latency_ms = Column(Integer)
    reachable = Column(Boolean, default=False)

    connection = relationship("DBConnection", back_populates="health_checks")

