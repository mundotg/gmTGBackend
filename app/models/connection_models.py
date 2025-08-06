from sqlalchemy import Boolean, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from app.database import Base

class DBConnection(Base):
    __tablename__ = "db_connections"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    name = Column(String)
    type = Column(String)  # PostgreSQL, MySQL, etc.
    host = Column(String)
    port = Column(Integer)
    username = Column(String)
    password = Column(String)
    database_name = Column(String)
    sslmode = Column(String)
    service = Column(String)
    trustServerCertificate = Column(String)
    status = Column(String, default="available")
    created_at = Column(DateTime, default=datetime.now(timezone.utc))

    owner = relationship("User", back_populates="db_connections")
    queries = relationship("QueryHistory", back_populates="connection", cascade="all, delete-orphan")
    statistics = relationship("DBStatistics", back_populates="connection", uselist=False, passive_deletes=True)
    logs = relationship("ConnectionLog", back_populates="connection", passive_deletes=True)
    structures = relationship("DBStructure", back_populates="connection", cascade="all, delete-orphan", lazy="selectin")

    row_count_cache = relationship("TableRowCountCache", back_populates="connection", cascade="all, delete-orphan")

class ActiveConnection(Base):
    __tablename__ = "active_connection"

    connection_id = Column(Integer, ForeignKey("db_connections.id"), primary_key=True, nullable=False)
    activated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    status = Column(Boolean, default=False, nullable=False)
    connection = relationship("DBConnection", backref="active_status")

class ConnectionLog(Base):
    __tablename__ = "connection_logs"

    id = Column(Integer, primary_key=True, index=True)
    connection_id = Column(Integer, ForeignKey("db_connections.id",ondelete="SET NULL"),nullable=True)
    action = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.now(timezone.utc))
    status = Column(String)

    connection = relationship("DBConnection", back_populates="logs")
