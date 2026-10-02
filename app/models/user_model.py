from datetime import datetime
from typing import List, Optional, Optional
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Table,
    Text,
    UniqueConstraint,
    Index,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, mapped_column, relationship
from app.database import Base
from app.ultils.company_permissions import is_company_permission
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
# 📧 Token de Confirmação de E-mail
# =============================
class EmailVerificationToken(Base):
    __tablename__ = "email_verification_tokens"

    id = Column(Integer, primary_key=True, index=True)
    token = Column(String, unique=True, index=True, nullable=False)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False,
        index=True,
    )
    # 📅 Data criada
    created_at = Column(DateTime(timezone=True), default=func.now(), nullable=False)
    # ⏳ Prazo / Data limite de validade
    expires_at = Column(DateTime(timezone=True), nullable=False)
    # ✅ Data em que foi validado
    validated_at = Column(DateTime(timezone=True), nullable=True)
    # 🚩 Status de utilização
    is_used = Column(Boolean, default=False, nullable=False)

    user = relationship("User", back_populates="email_verification_tokens")

    def __repr__(self):
        return (
            f"<EmailVerificationToken(id={self.id}, user_id={self.user_id}, "
            f"is_used={self.is_used}, expires_at={self.expires_at})>"
        )


# =============================
# 🌐 Contas OAuth / Provedores Externos
# =============================
class OAuthAccount(Base):
    __tablename__ = "oauth_accounts"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False,
        index=True,
    )
    provider = Column(String(50), nullable=False, index=True)  # google, github, gitlab, microsoft, auth0
    provider_user_id = Column(String(255), nullable=False, index=True)
    email = Column(String(255), nullable=True, index=True)
    name = Column(String(255), nullable=True)
    avatar_url = Column(String, nullable=True)
    access_token = Column(String, nullable=True)
    refresh_token = Column(String, nullable=True)
    expires_at = Column(DateTime(timezone=True), nullable=True)
    raw_data = Column(Text, nullable=True)  # JSON com payload completo recebido do provedor
    created_at = Column(DateTime(timezone=True), default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("provider", "provider_user_id", name="uq_oauth_provider_user_id"),
    )

    user = relationship("User", back_populates="oauth_accounts")

    def __repr__(self):
        return f"<OAuthAccount(provider='{self.provider}', provider_user_id='{self.provider_user_id}', user_id={self.user_id})>"


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
    roles = relationship("Role", back_populates="empresa", cascade="all, delete-orphan")
    connections = relationship(
        "DBConnection",
        secondary="empresa_connections",
        back_populates="empresas",
        lazy="selectin",
    )

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
    name = Column(String(50), nullable=False)
    description = Column(String(200))
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    empresa_id = Column(
        Integer,
        ForeignKey("empresas.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    # Quem tem esta função como TIPO DE UTILIZADOR (users.role_id). Os membros
    # que a têm como cargo da empresa estão em `cargo_users` (users.empresa_role_id).
    #
    # passive_deletes="all": ao apagar uma função, quem limpa as referências é a
    # base de dados (ON DELETE SET NULL). Sem isto o SQLAlchemy carregava estes
    # membros e punha-lhes a FK a NULL no flush — por cima da transferência
    # para outra função que delete_role acabou de fazer.
    users = relationship(
        "User", back_populates="role", foreign_keys="User.role_id", passive_deletes="all"
    )
    cargo_users = relationship(
        "User",
        back_populates="empresa_role",
        foreign_keys="User.empresa_role_id",
        passive_deletes="all",
    )
    empresa = relationship("Empresa", back_populates="roles")

    permissions = relationship(
        "Permission",
        secondary="roles_permissions",
        back_populates="roles",
        lazy="select",
    )

    # Nome único entre as funções globais e, à parte, dentro de cada empresa
    # (migração c3d4e5f6a7b8). Índices parciais: um UNIQUE(name, empresa_id)
    # simples não trava duas globais com o mesmo nome, porque NULL ≠ NULL.
    __table_args__ = (
        Index(
            "uq_roles_name_global", "name", unique=True,
            postgresql_where=text("empresa_id IS NULL"),
            sqlite_where=text("empresa_id IS NULL"),
        ),
        Index(
            "uq_roles_name_empresa", "name", "empresa_id", unique=True,
            postgresql_where=text("empresa_id IS NOT NULL"),
            sqlite_where=text("empresa_id IS NOT NULL"),
        ),
    )

    def __repr__(self):
        return f"<Role(id={self.id}, name='{self.name}', empresa_id={self.empresa_id})>"


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
    # Tipo de utilizador: função GLOBAL do sistema (admin, manager, developer, user).
    role_id = Column(Integer, ForeignKey("roles.id", ondelete="SET NULL"))
    # Cargo: função da EMPRESA do membro (roles.empresa_id == users.empresa_id).
    # As permissões efetivas somam as duas (ver `permissions`).
    empresa_role_id = Column(
        Integer, ForeignKey("roles.id", ondelete="SET NULL"), nullable=True, index=True
    )
    settings = relationship(
        "Settings", back_populates="user", uselist=False, cascade="all, delete-orphan"
    )

    empresa = relationship("Empresa", back_populates="users")
    cargo = relationship("Cargo", back_populates="users")
    role = relationship("Role", back_populates="users", foreign_keys=[role_id])
    empresa_role = relationship(
        "Role", back_populates="cargo_users", foreign_keys=[empresa_role_id]
    )

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
    # 🔗 Tokens e Contas OAuth
    refresh_tokens = relationship(
        "RefreshToken", back_populates="user", cascade="all, delete-orphan"
    )
    email_verification_tokens = relationship(
        "EmailVerificationToken", back_populates="user", cascade="all, delete-orphan"
    )
    oauth_accounts = relationship(
        "OAuthAccount", back_populates="user", cascade="all, delete-orphan"
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

    chat_sessions = relationship("ChatSession", back_populates="user")

    @property
    def permissions(self) -> set[str]:
        """
        Tipo de utilizador (função global) + cargo da empresa.

        O cargo só conta se for mesmo da empresa atual do membro, e só com
        permissões relacionadas com a empresa (app/ultils/company_permissions):
        nunca `admin:*` nem acessos da plataforma, mesmo que a função os tenha.
        """
        perms: set[str] = set()
        if self.role and self.role.permissions:
            perms |= {p.name for p in self.role.permissions}
        cargo = self.empresa_role
        if cargo and cargo.permissions and cargo.empresa_id is not None and cargo.empresa_id == self.empresa_id:
            perms |= {p.name for p in cargo.permissions if is_company_permission(p.name)}
        return perms

    def __repr__(self):
        return f"<User(id='{self.id}', email='{self.email}')>"


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
