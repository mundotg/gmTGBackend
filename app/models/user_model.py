from datetime import datetime
from typing import List, Optional, Optional
from sqlalchemy import (
    JSON,
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, mapped_column, relationship
from app.database import Base
from app.models.clouds_models import FileModel, Plan, Plan, StorageUsage
from app.models.task_models import TimestampMixin, project_team_association


# =============================
# 🔐 Refresh Token
# =============================
class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id = Column(Integer, primary_key=True, index=True)
    token = Column(String, unique=True, index=True, nullable=False)
    user_IP = Column(String(45), nullable=True)  # Armazena o IP do usuário (opcional)
    user_agent = Column(
        String(500), nullable=True
    )  # Armazena o User-Agent do usuário (opcional)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False,
        index=True,
    )
    is_active = Column(Boolean, default=True)
    revoked = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    expires_at = Column(DateTime, nullable=False)

    user = relationship("User", back_populates="refresh_tokens")

    def __repr__(self):
        return f"<RefreshToken(user_id={self.user_id}, revoked={self.revoked})>"


# =============================
# 🏢 Empresa
# =============================
class Empresa(Base):
    __tablename__ = "empresas"

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(150), unique=True, nullable=False)
    tamanho = Column(String(50))
    nif = Column(String(50), unique=True)
    endereco = Column(String(255))
    criado_em = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    users = relationship("User", back_populates="empresa")

    def __repr__(self):
        return f"<Empresa(id={self.id}, nome='{self.nome}')>"


# =============================
# 🧩 Cargo
# =============================
class Cargo(Base):
    __tablename__ = "cargos"

    id = Column(Integer, primary_key=True, index=True)
    nome = Column(String(100), unique=True, nullable=False)
    descricao = Column(String(255))
    nivel = Column(String(50))
    criado_em = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    users = relationship("User", back_populates="cargo")

    def __repr__(self):
        return f"<Cargo(id={self.id}, nome='{self.nome}')>"


# =============================
# 🔑 Role (RBAC)
# =============================
class Role(Base):
    __tablename__ = "roles"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(200))
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    users = relationship("User", back_populates="role")

    permissions = relationship(
        "Permission",
        secondary="roles_permissions",
        back_populates="roles",
        lazy="select",
    )

    def __repr__(self):
        return f"<Role(id={self.id}, name='{self.name}')>"


# =============================
# 👤 User (UNIFICADO)
# =============================
class User(Base, TimestampMixin):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)

    # Dados pessoais
    nome = Column(String(255), nullable=False)
    apelido = Column(String(100))
    email = Column(String(255), unique=True, nullable=False, index=True)
    telefone = Column(String(30))
    avatar_url = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Segurança
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    email_verified = Column(Boolean, default=False)
    concorda_termos = Column(Boolean, default=False)

    # Relações organizacionais
    empresa_id = Column(Integer, ForeignKey("empresas.id", ondelete="SET NULL"))
    cargo_id = Column(Integer, ForeignKey("cargos.id", ondelete="SET NULL"))
    role_id = Column(Integer, ForeignKey("roles.id", ondelete="SET NULL"))
    settings = relationship(
        "Settings", back_populates="user", uselist=False, cascade="all, delete-orphan"
    )

    empresa = relationship("Empresa", back_populates="users")
    cargo = relationship("Cargo", back_populates="users")
    role = relationship("Role", back_populates="users")

    plan_id: Mapped[int] = mapped_column(ForeignKey("plans.id"))
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    plan: Mapped["Plan"] = relationship(back_populates="users")
    files: Mapped[List["FileModel"]] = relationship(back_populates="user")
    storage_usage: Mapped[Optional["StorageUsage"]] = relationship(
        back_populates="user", uselist=False
    )

    request_usage = relationship("RequestUsage", back_populates="user", uselist=False)
    network_metrics = relationship(
        "NetworkMetric", back_populates="user", uselist=False
    )
    # 🔗 Tokens
    refresh_tokens = relationship(
        "RefreshToken", back_populates="user", cascade="all, delete-orphan"
    )

    # 🔗 Projetos e tarefas
    created_projects = relationship(
        "Project", back_populates="owner_user", cascade="all, delete-orphan"
    )

    assigned_tasks = relationship(
        "Task", back_populates="assigned_user", foreign_keys="[Task.assigned_to_id]"
    )

    delegated_tasks = relationship(
        "Task", back_populates="delegated_user", foreign_keys="[Task.delegated_to_id]"
    )

    created_tasks = relationship(
        "Task", back_populates="creator_user", foreign_keys="[Task.created_by_id]"
    )

    created_sprints = relationship(
        "Sprint", back_populates="created_by", foreign_keys="[Sprint.created_by_id]"
    )

    # 🔗 Projetos em que participa
    projects_participating = relationship(
        "Project", secondary=project_team_association, back_populates="team_members"
    )

    db_connections = relationship(
        "DBConnection", back_populates="owner", cascade="all, delete-orphan"
    )
    query_history = relationship(
        "QueryHistory", back_populates="user", cascade="all, delete-orphan"
    )
    auth_providers = relationship(
        "UserAuthProvider", back_populates="user", cascade="all, delete-orphan"
    )

    chat_sessions = relationship("ChatSession", back_populates="user")

    @property
    def permissions(self) -> set[str]:
        if not self.role or not self.role.permissions:
            return set()
        return {permission.name for permission in self.role.permissions}

    def __repr__(self):
        return f"<User(id='{self.id}', email='{self.email}')>"


class UserAuthProvider(Base):
    __tablename__ = "user_auth_providers"

    __table_args__ = (
        UniqueConstraint("provider", "provider_user_id", name="uq_provider_user_id"),
    )

    id = Column(Integer, primary_key=True)

    user_id = Column(ForeignKey("users.id", ondelete="CASCADE"))

    provider = Column(String(50), nullable=False)

    provider_user_id = Column(String, nullable=False)
    location = Column(String, nullable=True)

    provider_email = Column(String)
    bio = Column(String, nullable=True)
    provider_username = Column(String)

    profile_url = Column(Text)

    access_token = Column(Text)

    refresh_token = Column(Text)

    token_expires_at = Column(DateTime)

    last_synced_at = Column(DateTime)

    provider_payload = Column(JSON)

    user = relationship("User", back_populates="auth_providers")


# =============================
# 🛡️ Permissão
# =============================
class Permission(Base):
    __tablename__ = "permissions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)

    roles = relationship(
        "Role", secondary="roles_permissions", back_populates="permissions"
    )

    def __repr__(self):
        return f"<Permission(name='{self.name}')>"


# =============================
# 🛡️ Role-Permission Association Table
# =============================
roles_permissions = Table(
    "roles_permissions",
    Base.metadata,
    Column(
        "role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True
    ),
    Column(
        "permission_id",
        Integer,
        ForeignKey("permissions.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)
