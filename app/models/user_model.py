from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, func
)
from sqlalchemy.orm import relationship
from app.database import Base


class RefreshToken(Base):
    """
    Representa um token de atualização (refresh token) emitido para um usuário.
    Usado para renovar o token de acesso sem exigir novo login.
    """
    __tablename__ = "refresh_tokens"

    id = Column(Integer, primary_key=True, index=True)
    token = Column(String, unique=True, index=True, nullable=False)

    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False,
        index=True
    )

    revoked = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    expires_at = Column(DateTime, nullable=False)

    # Relação inversa com o usuário
    user = relationship("User", back_populates="refresh_tokens")

    def __repr__(self):
        return f"<RefreshToken(user_id={self.user_id}, revoked={self.revoked})>"


# -----------------------------
# 🏢 Tabela: Empresa
# -----------------------------
class Empresa(Base):
    """
    Representa uma empresa no sistema.
    """
    __tablename__ = "empresas"

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(150), unique=True, nullable=False)
    tamanho = Column(String(50), nullable=True)
    nif = Column(String(50), unique=True, nullable=True)  # opcional
    endereco = Column(String(255), nullable=True)
    criado_em = Column(DateTime, default=datetime.utcnow)

    # 🔗 Relação inversa
    usuarios = relationship("User", back_populates="empresa_ref")

    def __repr__(self):
        return f"<Empresa(id={self.id}, nome='{self.nome}')>"


# -----------------------------
# 🧩 Tabela: Cargo
# -----------------------------
class Cargo(Base):
    """
    Representa o cargo/função de um usuário dentro da empresa.
    Exemplo: 'Desenvolvedor', 'Gerente', 'Analista', etc.
    """
    __tablename__ = "cargos"

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(100), unique=True, nullable=False)
    descricao = Column(String(255), nullable=True)
    nivel = Column(String(50), nullable=True)  # exemplo: júnior, pleno, sênior
    criado_em = Column(DateTime, default=datetime.utcnow)

    # 🔗 Relação inversa
    usuarios = relationship("User", back_populates="cargo_ref")

    def __repr__(self):
        return f"<Cargo(id={self.id}, nome='{self.nome}')>"


# -----------------------------
# 👤 Tabela: User
# -----------------------------
class User(Base):
    """
    Representa um usuário do sistema, com informações pessoais e corporativas.
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)

    # Informações pessoais
    nome = Column(String(100), nullable=False)
    apelido = Column(String(50), nullable=True)
    email = Column(String(120), unique=True, index=True, nullable=False)
    # userName = Column(String(100), unique=True, index=True, nullable=False)
    telefone = Column(String(30), nullable=True)
    # telefone2 =Column(String(30), nullable=True)

    # Relações com empresa e cargo
    empresa_id = Column(Integer, ForeignKey("empresas.id", ondelete="SET NULL"), nullable=True)
    cargo_id = Column(Integer, ForeignKey("cargos.id", ondelete="SET NULL"), nullable=True)

    # Segurança
    hashed_password = Column(String, nullable=False)

    # Termos
    concorda_termos = Column(Boolean, default=False, nullable=False)

    # 🔗 Relacionamentos
    empresa_ref = relationship("Empresa", back_populates="usuarios")
    cargo_ref = relationship("Cargo", back_populates="usuarios")

    refresh_tokens = relationship(
        "RefreshToken",
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True
    )
    db_connections = relationship(
        "DBConnection",
        back_populates="owner",
        cascade="all, delete-orphan"
    )
    query_history = relationship(
        "QueryHistory",
        back_populates="user",
        cascade="all, delete-orphan"
    )
    settings = relationship(
        "Settings",
        back_populates="user",
        cascade="all, delete-orphan"
    )

    criado_em = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<User(id={self.id}, email='{self.email}', empresa_id={self.empresa_id}, cargo_id={self.cargo_id})>"
