from datetime import datetime, timedelta
from enum import Enum
from typing import  Optional
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, EmailStr, Field


# -----------------------------
# ENUM - Papel (Role)
# -----------------------------
class UserRoleEnum(str, Enum):
    admin = "admin"
    user = "user"
    manager = "manager"
    membro = "membro"
    gerente = "gerente"


# -----------------------------
# SCHEMA - Role
# -----------------------------
class RoleSchema(BaseModel):
    id: Optional[str] = Field(default_factory=lambda: str(uuid4()))
    nome: UserRoleEnum = Field(default=UserRoleEnum.membro)
    descricao: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -----------------------------
# REQUEST - Login
# -----------------------------
class UserLoginSchema(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=6)

    model_config = ConfigDict(from_attributes=True)




# -----------------------------
# MINI SCHEMAS (para respostas leves)
# -----------------------------
class ProjectMiniSchema(BaseModel):
    id: str
    name: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class TaskMiniSchema(BaseModel):
    id: str
    title: Optional[str] = None
    status: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

class UsuarioMiniSchema(BaseModel):
    id: str
    nome: str
    email: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
    
class SprintMiniSchema(BaseModel):
    """Campos e validações comuns entre todos os schemas de Sprint."""

    name: str = Field(..., min_length=3, max_length=100, description="Nome da sprint")
    start_date: datetime = Field(
        default_factory=datetime.utcnow, description="Data de início da sprint"
    )
    end_date: datetime = Field(..., description="Data de término da sprint")
    goal: Optional[str] = Field(
        default=None, max_length=255, description="Objetivo principal da sprint"
    )

