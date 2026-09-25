# -----------------------------
# app/schemas/sprint_schemas.py
# -----------------------------
from datetime import datetime
from typing import List, Optional
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from app.schemas.userTask_schemas import OptStrId, StrId




# -----------------------------
# BASE
# -----------------------------
class BaseSprintSchema(BaseModel):
    """Campos e validações comuns entre todos os schemas de Sprint."""

    name: str = Field(..., min_length=3, max_length=100, description="Nome da sprint")
    start_date: datetime = Field(
        default_factory=datetime.utcnow, description="Data de início da sprint"
    )
    end_date: datetime = Field(..., description="Data de término da sprint")
    goal: Optional[str] = Field(
        default=None, max_length=255, description="Objetivo principal da sprint"
    )

    @field_validator("end_date")
    @classmethod
    def validate_dates(cls, end_date: datetime, info):
        start_date = info.data.get("start_date")
        if start_date and end_date <= start_date:
            raise ValueError("A data de término deve ser posterior à data de início.")
        return end_date

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
        extra="ignore",
    )


# -----------------------------
# CREATE
# -----------------------------
class SprintCreateSchema(BaseSprintSchema):
    """Schema para criação de sprint."""

    # Opcional: o `project_id` verdadeiro vem do caminho da rota
    # (`POST /sprints/{project_id}`) e sobrepõe-se ao que vier no corpo.
    project_id: OptStrId = Field(
        None, alias="projectId", description="ID do projeto associado"
    )


# -----------------------------
# UPDATE
# -----------------------------
class SprintUpdateSchema(BaseModel):
    """Schema para atualização parcial de uma sprint."""

    name: Optional[str] = Field(None, min_length=3, max_length=100)
    start_date: Optional[datetime] = Field(None, description="Data de início da sprint")
    end_date: Optional[datetime] = Field(None, description="Data de término da sprint")
    goal: Optional[str] = Field(None, max_length=255, description="Objetivo da sprint")
    is_active: Optional[bool] = Field(None, description="Indica se a sprint está ativa")
    cancelled: Optional[bool] = Field(None, description="Indica se a sprint foi cancelada")
    motivo_cancelamento: Optional[str] = Field(
        None, description="Motivo do cancelamento (caso tenha sido cancelada)"
    )
    project_id: OptStrId = Field(None, alias="projectId")

    @field_validator("end_date")
    @classmethod
    def validate_dates(cls, end_date: Optional[datetime], info):
        start_date = info.data.get("start_date")
        if start_date and end_date and end_date <= start_date:
            raise ValueError("A data de término deve ser posterior à data de início.")
        return end_date

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
        extra="ignore",
    )

# -----------------------------
# RESPONSE / FULL SCHEMA (versão simplificada)
# -----------------------------

from app.schemas.task_schema import TaskStatsSchema   
from app.schemas.userTask_schemas import UsuarioMiniSchema,ProjectMiniSchema, TaskMiniSchema

class SprintSchema(BaseSprintSchema):
    """Schema completo de uma sprint (para responses)."""

    id: StrId = Field(..., description="Identificador único da sprint")
    is_active: bool = Field(default=True, description="Indica se a sprint está ativa")
    cancelled: bool = Field(default=False, description="Indica se a sprint foi cancelada")
    motivo_cancelamento: Optional[str] = Field(
        default=None, description="Motivo do cancelamento (se aplicável)"
    )
    # AliasChoices para aceitar `projectId` na entrada e continuar a encontrar
    # `project_id` no objeto ORM na saída — com um `alias` simples, o Pydantic
    # procurava `sprint.projectId` e devolvia sempre None.
    project_id: OptStrId = Field(
        None, validation_alias=AliasChoices("projectId", "project_id")
    )
    created_by_id: OptStrId = Field(
        None,
        validation_alias=AliasChoices("createdById", "created_by_id"),
        description="ID do criador da sprint",
    )
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # 🔗 RELAÇÕES BÁSICAS (apenas IDs para evitar recursão)
    project: Optional[ProjectMiniSchema] = Field(
        default=None, description="Nome do projeto (apenas para display)"
    )
    created_by: Optional[UsuarioMiniSchema] = Field(
        default=None, description="Criador da sprint"
    )
    # `tasks` é uma coleção no ORM: declarar um único TaskMiniSchema fazia a
    # validação falhar assim que a sprint tivesse tarefas.
    tasks: List[TaskMiniSchema] = Field(
        default_factory=list, description="Tarefas na sprint"
    )
    # Também é uma coleção no ORM (uma linha por agregação guardada).
    task_stats: List[TaskStatsSchema] = Field(default_factory=list)
    tasks_count: int = Field(
        default=0, description="Número de tarefas na sprint"
    )
    completed_tasks_count: int = Field(
        default=0, description="Número de tarefas concluídas"
    )

    @field_validator("tasks_count", mode="before")
    @classmethod
    def default_zero(cls, v):
        return v or 0

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
        extra="ignore",
    )