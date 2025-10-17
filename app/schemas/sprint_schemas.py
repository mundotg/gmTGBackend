# -----------------------------
# app/schemas/sprint_schemas.py
# -----------------------------
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator


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

    project_id: str = Field(..., alias="projectId", description="ID do projeto associado")


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
    project_id: Optional[str] = Field(None, alias="projectId")

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
# RESPONSE / FULL SCHEMA
# -----------------------------
class SprintSchema(BaseSprintSchema):
    """Schema completo de uma sprint (para responses)."""

    id: str = Field(..., description="Identificador único da sprint (UUID)")
    is_active: bool = Field(default=True, description="Indica se a sprint está ativa")
    cancelled: bool = Field(default=False, description="Indica se a sprint foi cancelada")
    motivo_cancelamento: Optional[str] = Field(
        default=None, description="Motivo do cancelamento (se aplicável)"
    )
    project_id: str = Field(..., alias="projectId")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
        extra="ignore",
    )
