from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Table, func
)
from sqlalchemy.orm import relationship
from app.database import Base
from app.models.task_models import TimestampMixin,project_team_association


# =============================
# 🔐 Refresh Token
# =============================
class RefreshToken(Base):
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

    users = relationship("User", back_populates="role")

    permissions = relationship(
        "Permission",
        secondary="roles_permissions",
        back_populates="roles",
        lazy="select"
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
        "Settings",
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan"
    )

    empresa = relationship("Empresa", back_populates="users")
    cargo = relationship("Cargo", back_populates="users")
    role = relationship("Role", back_populates="users")


    # 🔗 Tokens
    refresh_tokens = relationship(
        "RefreshToken",
        back_populates="user",
        cascade="all, delete-orphan"
    )

    # 🔗 Projetos e tarefas
    created_projects = relationship(
        "Project",
        back_populates="owner_user",
        cascade="all, delete-orphan"
    )

    assigned_tasks = relationship(
        "Task",
        back_populates="assigned_user",
        foreign_keys="[Task.assigned_to_id]"
    )

    delegated_tasks = relationship(
        "Task",
        back_populates="delegated_user",
        foreign_keys="[Task.delegated_to_id]"
    )

    created_tasks = relationship(
        "Task",
        back_populates="creator_user",
        foreign_keys="[Task.created_by_id]"
    )

    created_sprints = relationship(
        "Sprint",
        back_populates="created_by",
        foreign_keys="[Sprint.created_by_id]"
    )

    # 🔗 Projetos em que participa
    projects_participating = relationship(
        "Project",
        secondary=project_team_association,
        back_populates="team_members"
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
    
    @property
    def permissions(self) -> set[str]:
        if not self.role or not self.role.permissions:
            return set()
        return {permission.name for permission in self.role.permissions}

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
        "Role",
        secondary="roles_permissions",
        back_populates="permissions"
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
        "role_id",
        Integer,
        ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True
    ),
    Column(
        "permission_id",
        Integer,
        ForeignKey("permissions.id", ondelete="CASCADE"),
        primary_key=True
    )
)

