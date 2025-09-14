from sqlalchemy import Column, ForeignKey, String, DateTime, Boolean, JSON
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database import Base

class Project(Base):
    __tablename__ = "projects"

    id = Column(String, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(String(255), nullable=True)
    owner = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    due_date = Column(DateTime, nullable=True)

    team = Column(JSON, default=[])  # lista de strings
    tasks = relationship("Task", back_populates="project", cascade="all, delete-orphan")
    sprint = relationship("Sprint", uselist=False, back_populates="project", cascade="all, delete-orphan")


class Task(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    description = Column(String(255), nullable=True)
    priority = Column(String(50), nullable=False, default="media")
    assigned_to = Column(String(255), nullable=False)
    delegated_to = Column(String(255), nullable=True)
    start_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime, nullable=False)
    estimated_hours = Column(String(50), nullable=True)
    tags = Column(JSON, default=[])
    status = Column(String(50), nullable=False, default="pendente")
    created_by = Column(String(255), nullable=False)
    completed_at = Column(DateTime, nullable=True)
    is_validated = Column(Boolean, default=False)
    schedule = Column(JSON, nullable=True)  # pode guardar repeat/until

    project_id = Column(String, ForeignKey("projects.id"), nullable=False)
    project = relationship("Project", back_populates="tasks")


class Sprint(Base):
    __tablename__ = "sprints"

    id = Column(String, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    start_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime, nullable=False)
    goal = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)

    project_id = Column(String, ForeignKey("projects.id"), nullable=False)
    project = relationship("Project", back_populates="sprint")
