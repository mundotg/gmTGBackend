from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict

# -----------------------------
# TASK
# -----------------------------
class TaskScheduleSchema(BaseModel):
    repeat: str = Field("nenhum", description="Frequência da tarefa")
    until: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class TaskSchema(BaseModel):
    id: Optional[str] = None
    title: str
    description: Optional[str] = None
    priority: str
    project_id: Optional[str] = Field(None, alias="project_id")
    assignedTo: str = Field(..., alias="assigned_to")
    delegatedTo: Optional[str] = Field(None, alias="delegated_to")
    startDate: datetime = Field(..., alias="start_date")
    endDate: datetime = Field(..., alias="end_date")
    estimatedHours: Optional[int] = Field(None, alias="estimated_hours")
    tags: Optional[List[str]] = []
    status: str
    createdBy: str = Field(..., alias="created_by")
    completedAt: Optional[datetime] = Field(None, alias="completed_at")
    isValidated: Optional[bool] = Field(False, alias="is_validated")
    schedule: Optional[TaskScheduleSchema] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -----------------------------
# SPRINT
# -----------------------------
class SprintSchema(BaseModel):
    id: Optional[str] = None
    name: str
    startDate: datetime = Field(..., alias="start_date")
    endDate: datetime = Field(..., alias="end_date")
    goal: Optional[str] = None
    isActive: bool = Field(True, alias="is_active")

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -----------------------------
# PROJECT
# -----------------------------
class ProjectSchema(BaseModel):
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    owner: str
    team: List[str] = []
    tasks: List[TaskSchema] = []
    sprint: Optional[SprintSchema] = None
    created_at: datetime = Field(..., alias="created_at")
    due_date: Optional[datetime] = Field(None, alias="due_date")

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
