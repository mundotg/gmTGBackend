from enum import Enum
from typing import List, Optional, Union
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, field_validator
import uuid

# -----------------------------
# ENUMS
# -----------------------------
class TaskRepeatEnum(str, Enum):
    nenhum = "nenhum"
    diario = "diario"
    semanal = "semanal"
    mensal = "mensal"


class TaskPriorityEnum(str, Enum):
    baixa = "baixa"
    media = "media"
    alta = "alta"
    critica = "critica"
    urgente = "urgente"


class TaskStatusEnum(str, Enum):
    pendente = "pendente"
    em_andamento = "em_andamento"
    concluida = "concluida"
    cancelada = "cancelada"
    em_revisao = "em_revisao"
    bloqueada = "bloqueada"


# -----------------------------
# TASK SCHEDULE
# -----------------------------
class TaskSchedule(BaseModel):
    repeat: TaskRepeatEnum = TaskRepeatEnum.nenhum
    until: Optional[datetime] = None

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True
    )

    def to_json(self) -> dict:
        """Converte TaskSchedule em dicionário serializável"""
        return {
            "repeat": self.repeat.value,
            "until": self.until.isoformat() if self.until else None
        }

class TaskStatsSchema(BaseModel):
    total: int
    completed: int
    in_progress: int
    pending: int
    in_review: int
    blocked: int
    cancelled: int
    total_estimated_hours: float
    progress_percent: int
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        validate_assignment=True,
        extra='ignore'
    )
# -----------------------------
# TASK
# -----------------------------
class TaskSchema(BaseModel):
    id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    title: str = Field(..., max_length=255)
    description: Optional[str] = None
    priority: TaskPriorityEnum = TaskPriorityEnum.media
    start_date: datetime = Field(default_factory=datetime.utcnow, alias="startDate")
    end_date: datetime = Field(..., alias="endDate")
    estimated_hours: Optional[Union[str, int]] = Field(None, alias="estimatedHours")
    tags: Optional[List[str]] = Field(default_factory=list)
    status: TaskStatusEnum = TaskStatusEnum.pendente
    completed_at: Optional[datetime] = Field(None, alias="completedAt")
    is_validated:Optional[bool]  = Field(default=None, alias="isValidated")
    schedule: Optional[TaskSchedule] = None

    # 🔗 Relacionamentos
    assigned_to_id: str = Field(..., alias="assignedToId")
    delegated_to_id: Optional[str] = Field(None, alias="delegatedToId")
    created_by_id: str = Field(..., alias="createdById")
    project_id: str = Field(..., alias="projectId")

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
        validate_assignment=True,
        extra='ignore'
    )

    # -----------------------
    # VALIDADORES
    # -----------------------
    @field_validator('title')
    def validate_title(cls, v):
        if not v.strip():
            raise ValueError('O título é obrigatório.')
        return v.strip()

    @field_validator('end_date')
    def validate_end_date(cls, v, info):
        start = info.data.get("start_date")
        if start and v < start:
            raise ValueError("A data final não pode ser anterior à inicial.")
        return v

    @field_validator('tags')
    def clean_tags(cls, v):
        if not v:
            return []
        return [t.strip() for t in v if t.strip()]

    # -----------------------
    # CUSTOM SERIALIZATION
    # -----------------------
    def model_dump(self, **kwargs):
        """Garante que o dump sempre seja JSON-compatível"""
        data = super().model_dump(**kwargs)

        # Serializa schedule
        if isinstance(self.schedule, TaskSchedule):
            data["schedule"] = self.schedule.to_json()
        elif isinstance(data.get("schedule"), dict):
            until = data["schedule"].get("until")
            if isinstance(until, datetime):
                data["schedule"]["until"] = until.isoformat()

        # Serializa tags
        if isinstance(self.tags, list):
            data["tags"] = [t.strip() for t in self.tags if t.strip()]

        return data
