from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database import Base

class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id = Column(Integer, primary_key=True, index=True)
    token = Column(String, unique=True, index=True, nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"))
    revoked = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)

    user = relationship("User", back_populates="refresh_tokens")


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)

    # Informações Pessoais
    nome = Column(String)
    apelido = Column(String)
    email = Column(String, unique=True, index=True)
    telefone = Column(String)

    # Informações da Empresa
    nome_empresa = Column(String)
    cargo = Column(String, nullable=True)
    tamanho_empresa = Column(String, nullable=True)

    # Segurança
    hashed_password = Column(String)

    # Termos
    concorda_termos = Column(Boolean, default=False)
    
    refresh_tokens = relationship("RefreshToken", back_populates="user",)
    db_connections = relationship("DBConnection", back_populates="owner", cascade="all, delete-orphan")
    query_history = relationship("QueryHistory", back_populates="user")
    settings = relationship("Settings", back_populates="user", cascade="all, delete-orphan")
