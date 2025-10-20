import uuid
from sqlalchemy import (
    Column, Enum, Float, ForeignKey, Integer, String, DateTime, Boolean, JSON, Text, Table
)
from enum import Enum as PyEnum
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database import Base

class TimestampMixin:
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# 🔗 Associação N:N entre Project e Usuario
project_team_association = Table(
    "project_team_association",
    Base.metadata,
    Column("project_id", String, ForeignKey("projects.id", ondelete="CASCADE", onupdate="CASCADE"), primary_key=True),
    Column("user_id", String, ForeignKey("usuarios.id", ondelete="CASCADE", onupdate="CASCADE"), primary_key=True),
)

class Role(Base):
    __tablename__ = "roles"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()), nullable=False)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(200), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.utcnow())

    # 🔗 Relação inversa
    users = relationship("Usuario", back_populates="role_ref", lazy="select")

    def __repr__(self):
        return f"<Role(id={self.id}, name='{self.name}')>"
    
class Usuario(Base, TimestampMixin):
    __tablename__ = "usuarios"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    avatarUrl = Column(String, nullable=True)
    user_id = Column(String, nullable=True)
    nome = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False, unique=True)
    senha = Column(String(255), nullable=False)
    role_id = Column(String, ForeignKey("roles.id", ondelete="SET NULL"), nullable=True)
    role_ref = relationship("Role", back_populates="users", lazy="select")
    is_active = Column(Boolean, default=True)
    email_verified = Column(Boolean, default=False)

    # 🔗 Relações
    created_projects = relationship("Project", back_populates="owner_user", cascade="all, delete-orphan", lazy="select")
    assigned_tasks = relationship("Task", back_populates="assigned_user", foreign_keys="[Task.assigned_to_id]", lazy="select")
    delegated_tasks = relationship("Task", back_populates="delegated_user", foreign_keys="[Task.delegated_to_id]", lazy="select")
    created_tasks = relationship("Task", back_populates="creator_user", foreign_keys="[Task.created_by_id]", lazy="select")
    created_sprints = relationship("Sprint", back_populates="created_by", foreign_keys="[Sprint.created_by_id]", lazy="select")
    
    # 🔗 Projetos em que o usuário participa
    projects_participating = relationship(
        "Project",
        secondary=project_team_association,
        back_populates="team_members",
        lazy="select"
    )

    def __repr__(self):
        return f"<Usuario(id='{self.id}', nome='{self.nome}', email='{self.email}')>"
    
# -----------------------------
# Tabela: TypeProjecto
# -----------------------------
class TypeProjecto(Base):
    __tablename__ = "typeprojecto"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()), nullable=False)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(200), nullable=True)

# -----------------------------
# Tabela: Project
# -----------------------------
class Project(Base, TimestampMixin):
    __tablename__ = "projects"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    owner_id = Column(String, ForeignKey("usuarios.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    due_date = Column(DateTime, nullable=True)
    type_project_id = Column(String, ForeignKey("typeprojecto.id", ondelete="SET NULL"), nullable=True)

    type_project = relationship("TypeProjecto", backref="projects", lazy="select")

    # Associação com conexão de banco
    id_conexao_db = Column(Integer, ForeignKey("db_connections.id", ondelete="SET NULL"), nullable=True)
    is_active = Column(Boolean, default=True)
    cancel_reason = Column(String(255), nullable=True)
    cancelled_at = Column(DateTime, nullable=True)

    # Relações
    owner_user = relationship("Usuario", back_populates="created_projects", lazy="select")
    team_members = relationship("Usuario", secondary="project_team_association", back_populates="projects_participating", lazy="select")
    tasks = relationship("Task", back_populates="project", cascade="all, delete-orphan", lazy="select")
    sprints = relationship("Sprint", back_populates="project", cascade="all, delete-orphan", lazy="select")
    task_stats = relationship("TaskStats", back_populates="project", cascade="all, delete-orphan", lazy="select")

    db_connection = relationship("DBConnection", back_populates="projects", lazy="select")

    def __repr__(self):
        return f"<Project(id='{self.id}', name='{self.name}', owner_id='{self.owner_id}')>"


class Task(Base,TimestampMixin):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    priority = Column(String(50), nullable=False, default="media")
    start_date = Column(DateTime, default=lambda: datetime.utcnow())
    end_date = Column(DateTime, nullable=False)
    estimated_hours = Column(String(50), nullable=True)
    tags = Column(JSON, default=list)
    status = Column(String(50), nullable=False, default="pendente")
    completed_at = Column(DateTime, nullable=True)
    is_validated = Column(Boolean, default=None, nullable=True)
    comentario_is_validated =Column(String(255), default=None, nullable=True)
    schedule = Column(JSON, nullable=True)
    sprint_id = Column(String, ForeignKey("sprints.id", ondelete="SET NULL", onupdate="CASCADE"), nullable=True)
    is_active = Column(Boolean, default=True, nullable=True)
    cancel_reason = Column(String(255), nullable=True)
    cancelled_at = Column(DateTime, nullable=True)

    # 🔗 Relações com usuários
    assigned_to_id = Column(String, ForeignKey("usuarios.id", ondelete="SET NULL", onupdate="CASCADE"), nullable=False)
    delegated_to_id = Column(String, ForeignKey("usuarios.id", ondelete="SET NULL", onupdate="CASCADE"), nullable=True)
    created_by_id = Column(String, ForeignKey("usuarios.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)

    assigned_user = relationship("Usuario", back_populates="assigned_tasks", foreign_keys=[assigned_to_id], lazy="select")
    delegated_user = relationship("Usuario", back_populates="delegated_tasks", foreign_keys=[delegated_to_id], lazy="select")
    creator_user = relationship("Usuario", back_populates="created_tasks", foreign_keys=[created_by_id], lazy="select")

    # 🔗 Relação com projeto
    project_id = Column(String, ForeignKey("projects.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    sprint = relationship("Sprint", back_populates="tasks", lazy="select")
    project = relationship("Project", back_populates="tasks", lazy="select")

    def __repr__(self):
        return f"<Task(id='{self.id}', title='{self.title}', project_id='{self.project_id}')>"


class Sprint(Base, TimestampMixin):
    __tablename__ = "sprints"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(255), nullable=False)
    start_date = Column(DateTime, default=lambda: datetime.utcnow())
    end_date = Column(DateTime, nullable=False)
    goal = Column(String(255), nullable=True)

    # Status e cancelamento
    is_active = Column(Boolean, default=True)
    cancelled = Column(Boolean, default=False)
    motivo_cancelamento = Column(Text, nullable=True)

    # Relacionamento com usuário criador
    created_by_id = Column(String, ForeignKey("usuarios.id", ondelete="SET NULL"))
    created_by = relationship("Usuario", lazy="select")

    # Relacionamento com projeto
    project_id = Column(
        String,
        ForeignKey("projects.id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False
    )
    project = relationship("Project", back_populates="sprints", lazy="select")

    # Relacionamentos auxiliares
    tasks = relationship("Task", back_populates="sprint", cascade="all, delete-orphan", lazy="select")
    task_stats = relationship("TaskStats", back_populates="sprint", cascade="all, delete-orphan", lazy="select")

    def __repr__(self):
        return (
            f"<Sprint(id='{self.id}', name='{self.name}', "
            f"active={self.is_active}, cancelled={self.cancelled}, "
            f"project_id='{self.project_id}')>"
        )
    

class TaskStats(Base):
    """
    Armazena estatísticas agregadas de tarefas por projeto e/ou sprint.
    Pode ser atualizada periodicamente (ex: via job ou após update em Task).
    """
    __tablename__ = "task_stats"

    id = Column(String, primary_key=True, index=True, default=lambda: str(uuid.uuid4()))

    # 🔗 Referências
    project_id = Column(String, ForeignKey("projects.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    sprint_id = Column(String, ForeignKey("sprints.id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)

    # 📊 Dados agregados
    total = Column(Integer, default=0)
    completed = Column(Integer, default=0)
    in_progress = Column(Integer, default=0)
    pending = Column(Integer, default=0)
    in_review = Column(Integer, default=0)
    blocked = Column(Integer, default=0)
    cancelled = Column(Integer, default=0)
    validated = Column(Integer, default=0)

    total_estimated_hours = Column(Float, default=0.0)
    progress_percent = Column(Integer, default=0)
    avg_completion_time = Column(Float, default=0.0)
    overdue_tasks = Column(Integer, default=0)

    # 🕓 Controle de atualização
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # 🔗 Relações (opcionais)
    project = relationship("Project", back_populates="task_stats", lazy="joined")
    sprint = relationship("Sprint", back_populates="task_stats", lazy="joined")

    def __repr__(self):
        return f"<TaskStats(project_id={self.project_id}, sprint_id={self.sprint_id}, total={self.total})>"

class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("usuarios.id"))
    user = relationship("Usuario", lazy="select")
    action = Column(String(255))
    entity = Column(String(100))
    entity_id = Column(String(100))
    timestamp = Column(DateTime, default=datetime.utcnow)