from sqlalchemy import (
    Column, Float, ForeignKey, Integer, String, DateTime, Boolean, Text, Table
)
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
    Column(
        "project_id",
        Integer,
        ForeignKey("projects.id", ondelete="CASCADE", onupdate="CASCADE"),
        primary_key=True
    ),
    Column(
        "user_id",
        Integer,
        ForeignKey("users.id", ondelete="CASCADE", onupdate="CASCADE"),
        primary_key=True
    ),
)


# -----------------------------
# Tabela: TypeProjecto
# -----------------------------
class TypeProjecto(Base):
    __tablename__ = "typeprojecto"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(200), nullable=True)

# -----------------------------
# Tabela: Project
# -----------------------------
class Project(Base, TimestampMixin):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    owner_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False
    )

    due_date = Column(DateTime)
    type_project_id = Column(
        Integer,
        ForeignKey("typeprojecto.id", ondelete="SET NULL")
    )

    type_project = relationship("TypeProjecto", backref="projects", lazy="select")

    id_conexao_db = Column(Integer, ForeignKey("db_connections.id", ondelete="SET NULL"))
    is_active = Column(Boolean, default=True)
    cancel_reason = Column(String(255))
    cancelled_at = Column(DateTime)

    owner_user = relationship("User", back_populates="created_projects")
    team_members = relationship(
        "User",
        secondary=project_team_association,
        back_populates="projects_participating"
    )

    tasks = relationship("Task", back_populates="project", cascade="all, delete-orphan")
    sprints = relationship("Sprint", back_populates="project", cascade="all, delete-orphan")
    task_stats = relationship("TaskStats", back_populates="project", cascade="all, delete-orphan")

    db_connection = relationship("DBConnection", back_populates="projects")

class Task(Base, TimestampMixin):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text)
    priority = Column(String(50), default="media")
    start_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime, nullable=False)

    status = Column(String(50), default="pendente")
    completed_at = Column(DateTime)

    sprint_id = Column(Integer, ForeignKey("sprints.id", ondelete="SET NULL"))

    assigned_to_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True
    )
    delegated_to_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL")
    )
    created_by_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False
    )

    assigned_user = relationship("User", foreign_keys=[assigned_to_id])
    delegated_user = relationship("User", foreign_keys=[delegated_to_id])
    creator_user = relationship("User", foreign_keys=[created_by_id])

    project_id = Column(
        Integer,
        ForeignKey("projects.id", ondelete="CASCADE")
    )

    sprint = relationship("Sprint", back_populates="tasks")
    project = relationship("Project", back_populates="tasks")

class Sprint(Base, TimestampMixin):
    __tablename__ = "sprints"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    start_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime, nullable=False)

    created_by_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL")
    )
    created_by = relationship("User")

    project_id = Column(
        Integer,
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False
    )

    project = relationship("Project", back_populates="sprints")
    tasks = relationship("Task", back_populates="sprint", cascade="all, delete-orphan")
    task_stats = relationship("TaskStats", back_populates="sprint", cascade="all, delete-orphan")

    

class TaskStats(Base):
    """
    Armazena estatísticas agregadas de tarefas por projeto e/ou sprint.
    Pode ser atualizada periodicamente (ex: via job ou após update em Task).
    """
    __tablename__ = "task_stats"

    id = Column(Integer, primary_key=True, index=True)

    # 🔗 Referências
    project_id = Column(Integer, ForeignKey("projects.id"))
    sprint_id = Column(Integer, ForeignKey("sprints.id"))

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

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(
    Integer,
    ForeignKey("users.id", ondelete="SET NULL"),
    nullable=True
)
    user = relationship("User")
    action = Column(String(255))
    entity = Column(String(100))
    entity_id = Column(String(100))
    timestamp = Column(DateTime, default=datetime.utcnow)
