from enum import Enum
from typing import Dict, List, Optional, Union
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.schemas.userTask_schemas import OptStrId, StrId


# ------------------------------------------------
# ENUMS
# ------------------------------------------------
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


# ------------------------------------------------
# SCHEDULE
# ------------------------------------------------
class TaskSchedule(BaseModel):
    """Informações de repetição/agendamento de uma tarefa."""
    repeat: TaskRepeatEnum = TaskRepeatEnum.nenhum
    until: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    def to_json(self) -> dict:
        """Converte TaskSchedule em formato serializável."""
        return {
            "repeat": self.repeat.value,
            "until": self.until.isoformat() if self.until else None
        }


# ------------------------------------------------
# ESTATÍSTICAS
# ------------------------------------------------
class TaskStatsSchema(BaseModel):
    """Estatísticas agregadas de tarefas."""
    total: int = 0
    completed: int = 0
    in_progress: int = 0
    pending: int = 0
    in_review: int = 0
    blocked: int = 0
    cancelled: int = 0
    validated: int = 0
    overdue_tasks: int = 0
    total_estimated_hours: float = 0.0
    progress_percent: int = 0

    # Contagem por prioridade — o painel de tarefas já a mostrava, mas nunca
    # vinha do servidor.
    priority_counts: Dict[str, int] = Field(default_factory=dict)

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        validate_assignment=True,
        extra='ignore'
    )


# ------------------------------------------------
# CAMPOS COMUNS
# ------------------------------------------------
class TaskFieldsMixin(BaseModel):
    """Campos editáveis de uma tarefa, partilhados por criação e atualização."""

    description: Optional[str] = None
    priority: TaskPriorityEnum = TaskPriorityEnum.media
    start_date: Optional[datetime] = Field(default=None, alias="startDate")
    estimated_hours: Optional[float] = Field(None, alias="estimatedHours")
    tags: List[str] = Field(default_factory=list)
    status: TaskStatusEnum = TaskStatusEnum.pendente
    completed_at: Optional[datetime] = Field(None, alias="completedAt")
    schedule: Optional[TaskSchedule] = None

    # 🔗 Relacionamentos (IDs)
    assigned_to_id: OptStrId = Field(None, alias="assignedToId")
    delegated_to_id: OptStrId = Field(None, alias="delegatedToId")
    # Sem isto, o `sprintId` que o frontend envia era descartado em silêncio
    # (extra="ignore") e nenhuma tarefa chegava a entrar numa sprint.
    sprint_id: OptStrId = Field(None, alias="sprintId")

    @field_validator("tags")
    @classmethod
    def clean_tags(cls, v: Optional[List[str]]):
        return [t.strip() for t in v or [] if t and t.strip()]

    @field_validator("estimated_hours", mode="before")
    @classmethod
    def parse_hours(cls, v):
        """O formulário envia string ("8", "") ou número."""
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            raise ValueError("Horas estimadas devem ser um número.")


# ------------------------------------------------
# ENTRADA - CRIAÇÃO
# ------------------------------------------------
class TaskCreateSchema(TaskFieldsMixin):
    """
    O que o cliente envia ao criar uma tarefa.

    Não aceita `id` (a chave primária é gerada pela base de dados) nem
    `created_by_id` — o criador vem sempre da sessão autenticada, para o
    cliente não poder registar a tarefa em nome de outra pessoa.
    """

    title: str = Field(..., max_length=255)
    end_date: datetime = Field(..., alias="endDate")

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
        extra="ignore",
    )

    @field_validator("title")
    @classmethod
    def validate_title(cls, v: str):
        if not v.strip():
            raise ValueError("O título é obrigatório.")
        return v.strip()

    @field_validator("end_date")
    @classmethod
    def validate_end_date(cls, v: datetime, info):
        start = info.data.get("start_date")
        if start and v < start:
            raise ValueError("A data final não pode ser anterior à data inicial.")
        return v


# ------------------------------------------------
# ENTRADA - ATUALIZAÇÃO (parcial)
# ------------------------------------------------
class TaskUpdateSchema(TaskFieldsMixin):
    """
    Atualização parcial: tudo opcional. Combinado com `exclude_unset=True`,
    só o que o cliente enviou é escrito — enviar meia dúzia de campos deixa
    de apagar o resto.
    """

    title: Optional[str] = Field(None, max_length=255)
    end_date: Optional[datetime] = Field(None, alias="endDate")
    priority: Optional[TaskPriorityEnum] = None
    status: Optional[TaskStatusEnum] = None
    tags: Optional[List[str]] = None

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
        extra="ignore",
    )

    @field_validator("title")
    @classmethod
    def validate_title(cls, v: Optional[str]):
        if v is not None and not v.strip():
            raise ValueError("O título não pode ficar vazio.")
        return v.strip() if v else v


# ------------------------------------------------
# RESPONSE SCHEMA (COM RELACIONAMENTOS)
# ------------------------------------------------
from app.schemas.userTask_schemas import (
    UsuarioMiniSchema,
    ProjectMiniSchema,
    SprintMiniSchema,
)


class TaskSchema(BaseModel):
    """
    Schema completo para retorno de tarefa (com usuários e projeto relacionados).

    ⚠️ Sem aliases camelCase, de propósito. Com `from_attributes`, o Pydantic v2
    procura o atributo no objeto ORM **pelo alias**: um campo declarado como
    `Field(None, alias="estimatedHours")` ia procurar `task.estimatedHours`,
    não encontrava, e devolvia None — o valor estava gravado mas nunca chegava
    ao frontend. O frontend também lê tudo em snake_case (ver `Task` em
    app/task/types), por isso os nomes coincidem.
    """

    id: StrId
    title: str
    description: Optional[str] = None
    priority: Optional[TaskPriorityEnum] = TaskPriorityEnum.media
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    estimated_hours: Optional[float] = None
    tags: List[str] = Field(default_factory=list)
    status: Optional[TaskStatusEnum] = TaskStatusEnum.pendente
    completed_at: Optional[datetime] = None
    schedule: Optional[TaskSchedule] = None

    # ✅ Validação
    is_validated: Optional[bool] = None
    comentario_is_validated: Optional[str] = None
    validated_at: Optional[datetime] = None

    # 🔗 IDs
    assigned_to_id: OptStrId = None
    delegated_to_id: OptStrId = None
    created_by_id: OptStrId = None
    project_id: OptStrId = None
    sprint_id: OptStrId = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # 🔗 Relações
    assigned_user: Optional[UsuarioMiniSchema] = Field(None, description="Usuário atribuído")
    delegated_user: Optional[UsuarioMiniSchema] = Field(None, description="Usuário delegado")
    creator_user: Optional[UsuarioMiniSchema] = Field(None, description="Criador da tarefa")
    validator_user: Optional[UsuarioMiniSchema] = Field(None, description="Quem validou")
    project: Optional[ProjectMiniSchema] = Field(None, description="Projeto associado")
    sprint: Optional[SprintMiniSchema] = Field(None, description="Sprint associada")

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
        extra="ignore",
    )

    @field_validator("tags", mode="before")
    @classmethod
    def normalize_tags(cls, v):
        """A coluna é JSON e pode vir a NULL em registos antigos."""
        return v or []


# Compatibilidade: código antigo importava `TaskBaseSchema`.
TaskBaseSchema = TaskCreateSchema
